package com.notification.notifier.service;

import com.notification.enums.InvoiceStatus;
import com.notification.events.InvoiceDueEvent;
import com.notification.notifier.client.InvoiceServiceClient;
import com.notification.notifier.exception.PermanentNotificationException;
import com.notification.notifier.exception.TransientNotificationException;
import feign.FeignException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class InvoiceEligibilityService {

    private final InvoiceServiceClient invoiceServiceClient;

    /**
     * @return true if the invoice no longer needs a notification (already paid or cancelled)
     * @throws PermanentNotificationException if the invoice does not exist
     * @throws TransientNotificationException if the status could not be fetched
     */
    public boolean shouldSkip(InvoiceDueEvent event) {
        try {
            var status = invoiceServiceClient.getStatus(event.invoiceId()).status();

            if (status == InvoiceStatus.PAID || status == InvoiceStatus.CANCELLED) {
                log.info("Invoice {} already {}, skipping eventId={}", event.invoiceId(), status, event.eventId());
                return true;
            }
            return false;

        } catch (FeignException.NotFound e) {
            throw new PermanentNotificationException("Invoice not found: " + event.invoiceId(), e);
        } catch (Exception e) {
            log.warn("Invoice status check failed for {}, will retry", event.invoiceId(), e);
            throw new TransientNotificationException("Invoice status check failed", e);
        }
    }
}
