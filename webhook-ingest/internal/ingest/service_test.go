package ingest_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/convin/webhook-ingest/internal/config"
	"github.com/convin/webhook-ingest/internal/ingest"
	"github.com/convin/webhook-ingest/internal/redisclient"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/testutil"
)

// eventJSON builds a well-formed call-completion payload.
func eventJSON(eventID, callID, accountID string) string {
	return fmt.Sprintf(`{
	  "event_id":      %q,
	  "call_id":       %q,
	  "account_id":    %q,
	  "status":        "completed",
	  "duration_sec":  143,
	  "recording_url": "https://recordings.example.com/%s.wav",
	  "occurred_at":   "2026-08-13T09:12:00Z"
	}`, eventID, callID, accountID, callID)
}

func post(t *testing.T, url, body string) *http.Response {
	t.Helper()
	resp, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("post: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

func TestWebhookStoresEventAndCall(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("got %d, want 200", resp.StatusCode)
	}

	exists, err := st.EventExists(ctx, eventID)
	if err != nil {
		t.Fatalf("EventExists: %v", err)
	}
	if !exists {
		t.Fatal("expected the event to be stored")
	}

	var gotAccount string
	row := st.Pool().QueryRow(ctx, `SELECT account_id FROM calls WHERE call_id = $1`, callID)
	if err := row.Scan(&gotAccount); err != nil {
		t.Fatalf("expected a call record for %s: %v", callID, err)
	}
	if gotAccount != accountID {
		t.Fatalf("call belongs to %q, want %q", gotAccount, accountID)
	}
}

func TestDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	for i := 0; i < 3; i++ {
		if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
			t.Fatalf("delivery %d: got %d, want 200", i, resp.StatusCode)
		}
	}

	var n int
	row := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if n != 1 {
		t.Fatalf("stored %d copies of %s, want 1", n, eventID)
	}
}

// TestConcurrentDuplicateDeliveryIsIgnored verifies the Redis lock serializes
// truly concurrent deliveries of the same event_id so that exactly one copy is
// stored and the account stats increment by exactly one.
func TestConcurrentDuplicateDeliveryIsIgnored(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)

	const n = 50
	errc := make(chan error, n)
	for i := 0; i < n; i++ {
		go func() {
			resp, err := http.Post(srv.URL+"/webhooks/calls", "application/json", strings.NewReader(body))
			if err != nil {
				errc <- err
				return
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				errc <- fmt.Errorf("got %d, want 200", resp.StatusCode)
				return
			}
			errc <- nil
		}()
	}

	for i := 0; i < n; i++ {
		if err := <-errc; err != nil {
			t.Fatalf("concurrent delivery failed: %v", err)
		}
	}

	var events int
	if err := st.Pool().QueryRow(ctx, `SELECT count(*) FROM events WHERE event_id = $1`, eventID).Scan(&events); err != nil {
		t.Fatalf("scan events: %v", err)
	}
	if events != 1 {
		t.Fatalf("stored %d copies of %s, want 1", events, eventID)
	}

	var calls int
	if err := st.Pool().QueryRow(ctx, `SELECT count(*) FROM calls WHERE call_id = $1`, callID).Scan(&calls); err != nil {
		t.Fatalf("scan calls: %v", err)
	}
	if calls != 1 {
		t.Fatalf("stored %d calls, want 1", calls)
	}

	var stats int
	if err := st.Pool().QueryRow(ctx, `SELECT call_count FROM account_stats WHERE account_id = $1`, accountID).Scan(&stats); err != nil {
		t.Fatalf("scan stats: %v", err)
	}
	if stats != 1 {
		t.Fatalf("account call_count=%d, want 1 (duplicate double-counted)", stats)
	}
}

// TestRecordingMarkedProcessed verifies the background worker marks the call's
// recording as processed shortly after ingestion.
func TestRecordingMarkedProcessed(t *testing.T) {
	srv, st := testutil.NewServer(t)
	eventID, callID, accountID := testutil.IDs(t, st)
	ctx := context.Background()

	body := eventJSON(eventID, callID, accountID)
	if resp := post(t, srv.URL+"/webhooks/calls", body); resp.StatusCode != http.StatusOK {
		t.Fatalf("ingest: got %d, want 200", resp.StatusCode)
	}

	// Poll for the worker to finish processing the recording.
	deadline := time.Now().Add(5 * time.Second)
	for {
		var processed bool
		err := st.Pool().QueryRow(ctx,
			`SELECT recording_processed FROM calls WHERE call_id = $1`, callID).Scan(&processed)
		if err != nil {
			t.Fatalf("scan recording_processed: %v", err)
		}
		if processed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("recording was never marked processed")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// TestShutdownDrainsInFlightRecordings verifies that calling Shutdown processes
// any queued recordings before returning, so no work is lost on deploy.
func TestShutdownDrainsInFlightRecordings(t *testing.T) {
	cfg := config.Load()
	st := testutil.NewStore(t)

	rdb, err := redisclient.New(context.Background(), cfg.RedisAddr)
	if err != nil {
		t.Fatalf("connect to redis: %v", err)
	}
	t.Cleanup(func() { _ = rdb.Close() })

	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	svc := ingest.New(st, stats.NewCache(), rdb, log)

	accountID := "acc_" + strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())
	callID := "call_" + strings.NewReplacer("/", "_", " ", "_").Replace(t.Name())

	t.Cleanup(func() {
		ctx := context.Background()
		for _, table := range []string{"events", "calls", "account_stats"} {
			if _, err := st.Pool().Exec(ctx,
				"DELETE FROM "+table+" WHERE account_id = $1", accountID); err != nil {
				t.Logf("cleanup %s: %v", table, err)
			}
		}
	})

	evt := ingest.Event{
		EventID:      "evt_" + callID,
		CallID:       callID,
		AccountID:    accountID,
		Status:       "completed",
		DurationSec:  143,
		RecordingURL: "https://recordings.example.com/" + callID + ".wav",
		OccurredAt:   time.Date(2026, 8, 13, 9, 12, 0, 0, time.UTC),
	}
	if err := svc.Ingest(context.Background(), evt); err != nil {
		t.Fatalf("ingest: %v", err)
	}

	// Shutdown must block until the queued recording is processed.
	svc.Shutdown()

	var processed bool
	if err := st.Pool().QueryRow(context.Background(),
		`SELECT recording_processed FROM calls WHERE call_id = $1`, callID).Scan(&processed); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if !processed {
		t.Fatal("Shutdown returned before the recording was processed")
	}
}
