package com.notification.notifier.service;

import com.notification.enums.InvoiceStatus;
import com.notification.events.InvoiceDueEvent;
import com.notification.notifier.client.InvoiceServiceClient;
import com.notification.notifier.exception.TransientNotificationException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class InvoiceEligibilityService {

    private final InvoiceServiceClient invoiceServiceClient;
    private final IdempotencyService idempotencyService;

    public boolean shouldSkip(InvoiceDueEvent event) {
        try {
            var status = invoiceServiceClient.getStatus(event.invoiceId());

            if (status.status() == InvoiceStatus.PAID || status.status() == InvoiceStatus.CANCELLED) {
                log.info("Invoice {} already {}, skipping eventId={}",
                        event.invoiceId(), status, event.eventId());
                safeMarkAsSent(event);
                return true;
            }
            return false;

        } catch (Exception e) {
            log.warn("Invoice status check failed for {}, will retry", event.invoiceId(), e);
            throw new TransientNotificationException("Invoice status check failed", e);
        }
    }

    private void safeMarkAsSent(InvoiceDueEvent event) {
        try { idempotencyService.markAsSent(event.eventId()); }
        catch (Exception ex) { log.warn("markAsSent failed for skipped event", ex); }
    }
}