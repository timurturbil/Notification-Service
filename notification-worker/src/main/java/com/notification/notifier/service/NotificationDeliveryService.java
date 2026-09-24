package com.notification.notifier.service;

import com.notification.events.InvoiceDueEvent;
import com.notification.notifier.model.NotificationDelivery;
import com.notification.notifier.repository.NotificationDeliveryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationDeliveryService {

    private final NotificationDeliveryRepository repository;

    /**
     * Find existing notification delivery record.
     */
    public Optional<NotificationDelivery> findByEventId(UUID eventId) {
        return repository.findByEventId(eventId);
    }

    /**
     * Create or update notification delivery record with DELIVERED status.
     */
    @Transactional
    public NotificationDelivery recordDelivery(InvoiceDueEvent event) {
        Optional<NotificationDelivery> existing = findByEventId(event.eventId());

        if (existing.isPresent()) {
            NotificationDelivery delivery = existing.get();
            delivery.setStatus("DELIVERED");
            delivery.setAttemptCount(delivery.getAttemptCount() + 1);
            delivery.setLastError(null);
            log.debug("Updated notification delivery record for invoice: {}, channel: {}", event.invoiceId(), event.channel());
            return repository.save(delivery);
        } else {
            NotificationDelivery delivery = NotificationDelivery.builder()
                .invoiceId(event.invoiceId())
                .eventId(event.eventId())
                .channel(event.channel())
                .status("DELIVERED")
                .attemptCount(1)
                .build();
            log.debug("Created notification delivery record for invoice: {}, channel: {}", event.invoiceId(), event.channel());
            return repository.save(delivery);
        }
    }

    /**
     * Record failed delivery attempt.
     */
    @Transactional
    public NotificationDelivery recordFailure(InvoiceDueEvent event, String errorMsg) {
        Optional<NotificationDelivery> existing = findByEventId(event.eventId());
        
        if (existing.isPresent()) {
            NotificationDelivery delivery = existing.get();
            delivery.setStatus("FAILED");
            delivery.setAttemptCount(delivery.getAttemptCount() + 1);
            delivery.setLastError(errorMsg);
            log.debug("Updated notification failure record for invoice: {}, channel: {}, error: {}", 
                event.invoiceId(), event.channel(), errorMsg);
            return repository.save(delivery);
        } else {
            NotificationDelivery delivery = NotificationDelivery.builder()
                .invoiceId(event.invoiceId())
                .eventId(event.eventId())
                .channel(event.channel())
                .status("FAILED")
                .attemptCount(1)
                .lastError(errorMsg)
                .build();
            log.debug("Created notification failure record for invoice: {}, channel: {}", event.invoiceId(), event.channel());
            return repository.save(delivery);
        }
    }

    /**
     * Record that notification was sent to DLQ (dead letter queue).
     */
    @Transactional
    public NotificationDelivery recordDLQ(InvoiceDueEvent event, String errorMsg) {
        Optional<NotificationDelivery> existing = findByEventId(event.eventId());

        if (existing.isPresent()) {
            NotificationDelivery delivery = existing.get();
            delivery.setStatus("DLQ");
            delivery.setAttemptCount(delivery.getAttemptCount() + 1);
            delivery.setLastError(errorMsg);
            log.warn("Updated notification DLQ record for invoice: {}, channel: {}, error: {}", 
                event.invoiceId(), event.channel(), errorMsg);
            return repository.save(delivery);
        } else {
            NotificationDelivery delivery = NotificationDelivery.builder()
                .invoiceId(event.invoiceId())
                .eventId(event.eventId())
                .channel(event.channel())
                .status("DLQ")
                .attemptCount(1)
                .lastError(errorMsg)
                .build();
            log.warn("Created notification DLQ record for invoice: {}, channel: {}", event.invoiceId(), event.channel());
            return repository.save(delivery);
        }
    }
}
