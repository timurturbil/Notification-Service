ALTER TABLE notification_deliveries ALTER COLUMN invoice_id TYPE UUID USING invoice_id::UUID;

