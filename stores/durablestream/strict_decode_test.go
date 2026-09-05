package durablestream_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	eventbus "github.com/jilio/ebu"
	ds "github.com/jilio/ebu/stores/durablestream"
)

func TestStrictDecodingDoesNotSkipPoisonRecords(t *testing.T) {
	for _, poison := range []string{`"not an object"`, `null`, `{"type":42}`} {
		for _, mode := range []string{"read", "tail"} {
			t.Run(mode+poison, func(t *testing.T) {
				srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					if r.Method == http.MethodPut {
						w.WriteHeader(http.StatusCreated)
						return
					}
					w.Header().Set("Stream-Next-Offset", "03")
					w.Header().Set("Content-Type", "application/json")
					w.Header().Set("Stream-Up-To-Date", "true")
					_, _ = w.Write([]byte(`[{"id":"a","type":"valid","data":{}},` + poison + `,{"id":"c","type":"valid","data":{}}]`))
				}))
				defer srv.Close()
				store, err := ds.New(srv.URL, "strict", ds.WithStrictDecoding(), ds.WithDecodeErrorHandler(func(error, []byte) { t.Error("strict mode skipped an event") }))
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
				defer cancel()
				var decodeErr error
				switch mode {
				case "read":
					events, next, err := store.Read(ctx, eventbus.OffsetOldest, 0)
					decodeErr = err
					if len(events) != 0 || next != eventbus.OffsetOldest {
						t.Fatal("read advanced past a malformed record")
					}
				case "tail":
					for e, err := range store.Tail(ctx, eventbus.OffsetOldest) {
						if e != nil {
							t.Error("partial chunk escaped strict decoding")
						}
						decodeErr = err
						break
					}
				}
				if decodeErr == nil || !strings.Contains(decodeErr.Error(), "index 1") {
					t.Fatalf("expected indexed decoding error, got %v", decodeErr)
				}
			})
		}
	}
}

func TestStrictDecodingRejectsNullChunk(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPut {
			w.WriteHeader(http.StatusCreated)
			return
		}
		w.Header().Set("Stream-Next-Offset", "01")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`null`))
	}))
	defer srv.Close()
	store, err := ds.New(srv.URL, "strict", ds.WithStrictDecoding())
	if err != nil {
		t.Fatal(err)
	}
	events, offset, err := store.Read(context.Background(), eventbus.OffsetOldest, 0)
	if err == nil || len(events) != 0 || offset != eventbus.OffsetOldest {
		t.Fatalf("null chunk advanced: %v %q %v", events, offset, err)
	}
}
