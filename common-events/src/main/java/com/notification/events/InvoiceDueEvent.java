package com.notification.events;

import java.time.LocalDate;

public record InvoiceDueEvent(
    String eventId,
    String invoiceId,
    String userId,
    LocalDate dueDate,
    String channel,
    String templateId,
    int attempt
) {}
