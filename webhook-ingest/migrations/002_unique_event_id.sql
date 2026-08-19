-- Add UNIQUE constraint on events.event_id to prevent duplicate deliveries
-- at the database level. This is the foundation for idempotent ingestion.

-- First, drop the existing non-unique index
DROP INDEX IF EXISTS idx_events_event_id;

-- Add the unique constraint (creates a unique index implicitly)
ALTER TABLE events ADD CONSTRAINT uq_events_event_id UNIQUE (event_id);