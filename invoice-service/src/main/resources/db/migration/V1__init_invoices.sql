CREATE TABLE invoices (
    id UUID PRIMARY KEY,
    user_id VARCHAR(255) NOT NULL,
    amount NUMERIC(19, 2) NOT NULL,
    due_date DATE NOT NULL,
    status VARCHAR(50) NOT NULL,
    notification_scheduled BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL,
    updated_at TIMESTAMP NOT NULL
);

CREATE INDEX idx_due_date ON invoices(due_date);
CREATE INDEX idx_status ON invoices(status);
CREATE INDEX idx_notification_scheduled ON invoices(notification_scheduled);
