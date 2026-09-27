package com.notification.events;

import java.time.LocalDate;
import java.util.UUID;

public record InvoiceDueEvent(
    UUID eventId,
    UUID invoiceId,
    String userId,
    LocalDate dueDate,
    String channel,
    String templateId,
    int attempt
) {}
