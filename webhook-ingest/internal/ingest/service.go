// Package ingest accepts call-completion webhooks and processes them.
package ingest

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/convin/webhook-ingest/internal/redisclient"
	"github.com/convin/webhook-ingest/internal/stats"
	"github.com/convin/webhook-ingest/internal/store"
)

// Querier is the interface for database operations (both pool and tx).
type Querier = store.Querier

// recordingWork stands in for downloading and transcoding a recording.
const recordingWork = 50 * time.Millisecond

// idempotencyLockTTL is how long we hold the lock for a given event_id.
// Should be longer than the max expected ingestion time.
const idempotencyLockTTL = 10 * time.Second

// Service ingests webhook deliveries.
type Service struct {
	store *store.Store
	cache *stats.Cache
	rdb   *redis.Client
	log   *slog.Logger
}

// New builds a Service.
func New(s *store.Store, c *stats.Cache, rdb *redis.Client, log *slog.Logger) *Service {
	return &Service{store: s, cache: c, rdb: rdb, log: log}
}

// Stats returns the cached totals for an account.
func (s *Service) Stats(accountID string) stats.AccountStats {
	return s.cache.Get(accountID)
}

// Ingest stores a delivery and kicks off processing. Processing runs
// asynchronously so the provider gets a fast acknowledgement.
func (s *Service) Ingest(ctx context.Context, evt Event) error {
	// Acquire distributed lock for this event_id to serialize concurrent
	// deliveries of the same event. This prevents the TOCTOU race where
	// two requests both pass the idempotent check before either commits.
	lockKey := "ingest:lock:" + evt.EventID
	locked, err := redisclient.TryLock(ctx, s.rdb, lockKey, idempotencyLockTTL)
	if err != nil {
		return err
	}
	if !locked {
		// Another request is processing this event_id. Wait briefly and retry
		// the idempotent check - the other request will either commit or fail.
		s.log.Info("waiting for concurrent ingestion", "event_id", evt.EventID)
		return s.ingestWithRetry(ctx, evt)
	}

	// We hold the lock - proceed with ingestion
	defer func() {
		if err := redisclient.Unlock(ctx, s.rdb, lockKey); err != nil {
			s.log.Error("failed to release idempotency lock", "event_id", evt.EventID, "err", err)
		}
	}()

	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	// Use a transaction to ensure atomicity across events, calls, and stats.
	// The idempotent insert ensures duplicate event_ids don't double-count.
	var inserted bool
	err = s.store.WithTx(ctx, func(q store.Querier) error {
		inserted, err = s.store.InsertEventIdempotentTx(ctx, q, rec)
		if err != nil {
			return err
		}
		if !inserted {
			// Duplicate event - nothing more to do
			return nil
		}
		if err := s.store.UpsertCallTx(ctx, q, rec); err != nil {
			return err
		}
		if err := s.store.IncrementAccountStatsTx(ctx, q, rec.AccountID, rec.DurationSec); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	if !inserted {
		s.log.Info("duplicate delivery ignored", "event_id", evt.EventID)
		return nil
	}

	// Cache update happens after successful transaction commit
	s.cache.Record(rec.AccountID, rec.DurationSec)

	// Recordings are slow to fetch, so that part does not block the provider.
	if rec.RecordingURL != "" {
		go func() {
			if err := s.processRecording(context.Background(), rec); err != nil {
				// TODO: handle
			}
		}()
	}

	return nil
}

// ingestWithRetry is called when we couldn't acquire the lock.
// It waits briefly and then tries the idempotent insert directly
// (the other holder will have either committed or failed by then).
func (s *Service) ingestWithRetry(ctx context.Context, evt Event) error {
	// Small delay to let the lock holder complete
	select {
	case <-time.After(50 * time.Millisecond):
	case <-ctx.Done():
		return ctx.Err()
	}

	payload, err := json.Marshal(evt)
	if err != nil {
		return err
	}

	rec := store.Event{
		EventID:      evt.EventID,
		CallID:       evt.CallID,
		AccountID:    evt.AccountID,
		Status:       evt.Status,
		DurationSec:  evt.DurationSec,
		RecordingURL: evt.RecordingURL,
		OccurredAt:   evt.OccurredAt,
		Payload:      payload,
	}

	var inserted bool
	err = s.store.WithTx(ctx, func(q store.Querier) error {
		inserted, err = s.store.InsertEventIdempotentTx(ctx, q, rec)
		if err != nil {
			return err
		}
		if !inserted {
			return nil
		}
		if err := s.store.UpsertCallTx(ctx, q, rec); err != nil {
			return err
		}
		if err := s.store.IncrementAccountStatsTx(ctx, q, rec.AccountID, rec.DurationSec); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return err
	}

	if !inserted {
		s.log.Info("duplicate delivery ignored (after wait)", "event_id", evt.EventID)
		return nil
	}

	s.cache.Record(rec.AccountID, rec.DurationSec)

	if rec.RecordingURL != "" {
		go func() {
			if err := s.processRecording(context.Background(), rec); err != nil {
				// TODO: handle
			}
		}()
	}

	return nil
}

// processRecording downloads and transcodes the call recording, then marks
// the call as done.
func (s *Service) processRecording(ctx context.Context, rec store.Event) error {
	time.Sleep(recordingWork)
	return s.store.MarkRecordingProcessed(ctx, rec.CallID)
}
