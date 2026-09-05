//go:build ignore

// Run from the repository workspace: go run ./scripts/replication-smoke.go
// A writer process acknowledges a replication barrier, is killed, and loses its
// entire local directory. Recovery reads only the reopened destination database.
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	eventbus "github.com/jilio/ebu"
	"github.com/jilio/ebu/stores/sqlite"
)

const criticalRevision = 201

type remoteStore struct {
	url    string
	client *http.Client
}

func (s remoteStore) Append(ctx context.Context, e *eventbus.Event) (eventbus.Offset, error) {
	data, err := json.Marshal(e)
	if err != nil {
		return "", err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, strings.NewReader(string(data)))
	if err != nil {
		return "", err
	}
	response, err := s.client.Do(req)
	if err != nil {
		return "", err
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusOK {
		return "", fmt.Errorf("append: HTTP %d", response.StatusCode)
	}
	var offset eventbus.Offset
	err = json.NewDecoder(response.Body).Decode(&offset)
	return offset, err
}
func (remoteStore) Read(context.Context, eventbus.Offset, int) ([]*eventbus.StoredEvent, eventbus.Offset, error) {
	return nil, "", fmt.Errorf("write-only destination")
}

func writer(dir, url string) error {
	local, err := sqlite.New(filepath.Join(dir, "source.db"))
	if err != nil {
		return err
	}
	defer local.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	r, err := eventbus.NewReplicator(eventbus.ReplicatorConfig{Source: local, Destination: remoteStore{url, &http.Client{Timeout: 5 * time.Second}}, Checkpoints: local, ID: "smoke", Generation: "g1"}, eventbus.MirrorPollInterval(time.Millisecond))
	if err != nil {
		return err
	}
	go func() {
		if err := r.Run(ctx); err != nil {
			fmt.Fprintln(os.Stderr, err)
		}
	}()
	for i := 1; i <= criticalRevision+100; i++ {
		data, _ := json.Marshal(map[string]int{"revision": i})
		offset, err := local.Append(ctx, &eventbus.Event{ID: fmt.Sprint(i), Type: "revision", Data: data})
		if err != nil {
			return err
		}
		if i == criticalRevision {
			p, err := r.Position(offset)
			if err != nil {
				return err
			}
			wait, stop := context.WithTimeout(ctx, 15*time.Second)
			err = r.Wait(wait, p)
			stop()
			if err != nil {
				return err
			}
			fmt.Println("ACK")
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

func smoke() error {
	root, err := os.MkdirTemp("", "ebu-replication-smoke-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(root)
	source := filepath.Join(root, "worker")
	if err := os.Mkdir(source, 0700); err != nil {
		return err
	}
	destination := filepath.Join(root, "replica.db")
	replica, err := sqlite.New(destination)
	if err != nil {
		return err
	}
	defer replica.Close()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var e eventbus.Event
		if err := json.NewDecoder(req.Body).Decode(&e); err != nil {
			http.Error(w, err.Error(), 400)
			return
		}
		offset, err := replica.Append(req.Context(), &e)
		if err != nil {
			http.Error(w, err.Error(), 500)
			return
		}
		_ = json.NewEncoder(w).Encode(offset)
	}))
	defer server.Close()
	executable, err := os.Executable()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "writer", source, server.URL)
	cmd.Stderr = os.Stderr
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		return err
	}
	if err := cmd.Start(); err != nil {
		return err
	}
	waited := false
	defer func() {
		_ = cmd.Process.Kill()
		if !waited {
			_ = cmd.Wait()
		}
	}()
	ack := make(chan bool, 1)
	go func() { scanner := bufio.NewScanner(stdout); ack <- scanner.Scan() && scanner.Text() == "ACK" }()
	select {
	case ok := <-ack:
		if !ok {
			return fmt.Errorf("writer exited before confirmation")
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := cmd.Process.Kill(); err != nil {
		return err
	}
	_ = cmd.Wait()
	waited = true
	server.Close() // Join any destination append already accepted before the kill.
	if err := os.RemoveAll(source); err != nil {
		return err
	}
	if err := replica.Close(); err != nil {
		return err
	}
	recovered, err := sqlite.New(destination)
	if err != nil {
		return err
	}
	defer recovered.Close()
	events, _, err := recovered.Read(context.Background(), eventbus.OffsetOldest, 0)
	if err != nil {
		return err
	}
	seen := make(map[string]bool)
	for _, e := range events {
		var record struct {
			Revision int `json:"revision"`
		}
		if err := json.Unmarshal(e.Data, &record); err != nil {
			return err
		}
		if e.Type != "revision" || fmt.Sprint(record.Revision) != e.ID {
			return fmt.Errorf("corrupt replicated revision %q", e.ID)
		}
		seen[e.ID] = true
	}
	for i := 1; i <= criticalRevision; i++ {
		if !seen[fmt.Sprint(i)] {
			return fmt.Errorf("confirmed prefix lost revision %d", i)
		}
	}
	fmt.Printf("PASS: killed source process, removed its files, recovered all %d confirmed revisions from reopened replica\n", criticalRevision)
	return nil
}
func main() {
	var err error
	if len(os.Args) == 4 && os.Args[1] == "writer" {
		err = writer(os.Args[2], os.Args[3])
	} else {
		err = smoke()
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
