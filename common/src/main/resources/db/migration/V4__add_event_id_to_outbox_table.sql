ALTER TABLE outbox ADD COLUMN event_id UUID NOT NULL;
CREATE UNIQUE INDEX idx_outbox_event_id ON outbox(event_id);

