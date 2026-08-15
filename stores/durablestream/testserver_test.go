package durablestream_test

import (
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"time"

	durablestreams "github.com/durable-streams/durable-streams/packages/client-go"
)

// protoServer is a minimal in-memory Durable Streams server for tests,
// speaking the subset of the protocol the store exercises: idempotent PUT
// create (409 on config mismatch), POST append, HEAD metadata, GET catch-up
// and long-poll reads with chunking, DELETE. Offsets are zero-padded decimal
// message indices (lexicographically sortable, per spec §6); the protocol
// start-of-stream token and an absent offset both mean the beginning.
type protoServer struct {
	*httptest.Server
	mu         sync.Mutex
	streams    map[string]*protoStream
	chunkLimit int // max messages per GET; 0 = unlimited
}

type protoStream struct {
	contentType string
	messages    [][]byte
	notify      chan struct{}
}

func padOffset(n int) string {
	return fmt.Sprintf("%010d", n)
}

func parseProtoOffset(raw string) (int, bool) {
	if durablestreams.Offset(raw).IsStart() {
		return 0, true
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		return 0, false
	}
	if n < 0 {
		n = 0
	}
	return n, true
}

func mediaTypeOf(ct string) string {
	return strings.ToLower(strings.TrimSpace(strings.Split(ct, ";")[0]))
}

func newProtoServer() *protoServer {
	ps := &protoServer{streams: make(map[string]*protoStream)}
	mux := http.NewServeMux()
	mux.Handle("/v1/stream/", http.StripPrefix("/v1/stream/", http.HandlerFunc(ps.handle)))
	ps.Server = httptest.NewServer(mux)
	return ps
}

func (ps *protoServer) handle(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Path
	switch r.Method {
	case http.MethodPut:
		ct := r.Header.Get("Content-Type")
		if ct == "" {
			ct = "application/octet-stream"
		}
		ps.mu.Lock()
		defer ps.mu.Unlock()
		if st, ok := ps.streams[id]; ok {
			if mediaTypeOf(st.contentType) != mediaTypeOf(ct) {
				http.Error(w, "stream exists with different config", http.StatusConflict)
				return
			}
			w.WriteHeader(http.StatusOK)
			return
		}
		ps.streams[id] = &protoStream{contentType: ct, notify: make(chan struct{})}
		w.WriteHeader(http.StatusCreated)

	case http.MethodPost:
		body, _ := io.ReadAll(r.Body)
		if len(body) == 0 {
			http.Error(w, "empty append", http.StatusBadRequest)
			return
		}
		ps.mu.Lock()
		st, ok := ps.streams[id]
		if !ok {
			ps.mu.Unlock()
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		st.messages = append(st.messages, body)
		next := len(st.messages)
		close(st.notify)
		st.notify = make(chan struct{})
		ps.mu.Unlock()
		w.Header().Set("Stream-Next-Offset", padOffset(next))
		w.WriteHeader(http.StatusOK)

	case http.MethodHead:
		ps.mu.Lock()
		st, ok := ps.streams[id]
		if !ok {
			ps.mu.Unlock()
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		n := len(st.messages)
		ct := st.contentType
		ps.mu.Unlock()
		w.Header().Set("Content-Type", ct)
		w.Header().Set("Stream-Next-Offset", padOffset(n))
		w.WriteHeader(http.StatusOK)

	case http.MethodGet:
		off, ok := parseProtoOffset(r.URL.Query().Get("offset"))
		if !ok {
			http.Error(w, "bad offset", http.StatusBadRequest)
			return
		}
		live := r.URL.Query().Get("live") == "long-poll"

		ps.mu.Lock()
		st, streamOK := ps.streams[id]
		if !streamOK {
			ps.mu.Unlock()
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		n := len(st.messages)
		notify := st.notify
		ps.mu.Unlock()

		if off >= n && live {
			// Long-poll: wait briefly for new data, then time out with 204.
			select {
			case <-notify:
			case <-time.After(50 * time.Millisecond):
			case <-r.Context().Done():
				return
			}
			ps.mu.Lock()
			n = len(st.messages)
			ps.mu.Unlock()
		}

		if off >= n {
			w.Header().Set("Stream-Next-Offset", padOffset(n))
			w.Header().Set("Stream-Up-To-Date", "true")
			w.WriteHeader(http.StatusNoContent)
			return
		}

		ps.mu.Lock()
		end := n
		if ps.chunkLimit > 0 && off+ps.chunkLimit < n {
			end = off + ps.chunkLimit
		}
		parts := make([]string, 0, end-off)
		for _, m := range st.messages[off:end] {
			parts = append(parts, string(m))
		}
		ct := st.contentType
		ps.mu.Unlock()

		w.Header().Set("Content-Type", ct)
		w.Header().Set("Stream-Next-Offset", padOffset(end))
		if end == n {
			w.Header().Set("Stream-Up-To-Date", "true")
		}
		w.Write([]byte("[" + strings.Join(parts, ",") + "]"))

	case http.MethodDelete:
		ps.mu.Lock()
		defer ps.mu.Unlock()
		if _, ok := ps.streams[id]; !ok {
			http.Error(w, "not found", http.StatusNotFound)
			return
		}
		delete(ps.streams, id)
		w.WriteHeader(http.StatusOK)

	default:
		w.WriteHeader(http.StatusOK)
	}
}
