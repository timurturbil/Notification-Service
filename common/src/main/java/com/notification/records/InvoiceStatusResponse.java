package com.notification.records;

import com.notification.enums.InvoiceStatus;
import java.util.UUID;

public record InvoiceStatusResponse(UUID invoiceId, InvoiceStatus status) {
}
