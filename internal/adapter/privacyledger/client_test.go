package privacyledger

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestLedgerDeliveryAndAcknowledgement(t *testing.T) {
	t.Parallel()
	id, user := uuid.NewString(), uuid.NewString()
	var acked atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("X-Privacy-Service-Token") != "fixture" {
			t.Error("missing credential")
		}
		if r.Method == http.MethodPost {
			if r.URL.Path != "/internal/privacy/deletions/"+id+"/ack" {
				t.Error("wrong ack identity")
			}
			acked.Store(true)
			w.WriteHeader(204)

			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"watermark": 5, "next_after": 5, "events": []any{map[string]any{"event_id": id, "event_type": "user.deleted.v1", "schema_version": 1, "aggregate_id": user, "aggregate_version": 1, "occurred_at": time.Now().UTC(), "payload": map[string]any{"user_id": user, "request_id": id, "tombstone": true}}}})
	}))
	defer server.Close()
	client := New(server.URL, "fixture")
	page, err := client.Read(t.Context(), 0)
	if err != nil || len(page.Events) != 1 || page.Events[0].UserID.String() != user {
		t.Fatal(page, err)
	}
	if err = client.Ack(t.Context(), page.Events[0].EventID); err != nil || !acked.Load() {
		t.Fatal(err)
	}
	if _, err = client.Read(t.Context(), 5); err == nil {
		t.Fatal("non advancing event accepted")
	}
}
