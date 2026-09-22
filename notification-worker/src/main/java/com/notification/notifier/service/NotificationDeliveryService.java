package com.notification.notifier.service;

import com.notification.notifier.model.NotificationDelivery;
import com.notification.notifier.repository.NotificationDeliveryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.Optional;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationDeliveryService {

    private final NotificationDeliveryRepository repository;

    /**
     * Check if notification has been delivered for invoice+channel.
     */
    public boolean existsByInvoiceIdAndChannel(String invoiceId, String channel) {
        return repository.existsByInvoiceIdAndChannel(invoiceId, channel);
    }

    /**
     * Find existing notification delivery record.
     */
    public Optional<NotificationDelivery> findByInvoiceIdAndChannel(String invoiceId, String channel) {
        return repository.findByInvoiceIdAndChannel(invoiceId, channel);
    }

    /**
     * Create or update notification delivery record with DELIVERED status.
     */
    @Transactional
    public NotificationDelivery recordDelivery(String invoiceId, String channel) {
        Optional<NotificationDelivery> existing = findByInvoiceIdAndChannel(invoiceId, channel);
        
        if (existing.isPresent()) {
            NotificationDelivery delivery = existing.get();
            delivery.setStatus("DELIVERED");
            delivery.setAttemptCount(delivery.getAttemptCount() + 1);
            delivery.setLastError(null);
            log.debug("Updated notification delivery record for invoice: {}, channel: {}", invoiceId, channel);
            return repository.save(delivery);
        } else {
            NotificationDelivery delivery = NotificationDelivery.builder()
                .invoiceId(invoiceId)
                .channel(channel)
                .status("DELIVERED")
                .attemptCount(1)
                .build();
            log.debug("Created notification delivery record for invoice: {}, channel: {}", invoiceId, channel);
            return repository.save(delivery);
        }
    }

    /**
     * Record failed delivery attempt.
     */
    @Transactional
    public NotificationDelivery recordFailure(String invoiceId, String channel, String errorMsg) {
        Optional<NotificationDelivery> existing = findByInvoiceIdAndChannel(invoiceId, channel);
        
        if (existing.isPresent()) {
            NotificationDelivery delivery = existing.get();
            delivery.setStatus("FAILED");
            delivery.setAttemptCount(delivery.getAttemptCount() + 1);
            delivery.setLastError(errorMsg);
            log.debug("Updated notification failure record for invoice: {}, channel: {}, error: {}", 
                invoiceId, channel, errorMsg);
            return repository.save(delivery);
        } else {
            NotificationDelivery delivery = NotificationDelivery.builder()
                .invoiceId(invoiceId)
                .channel(channel)
                .status("FAILED")
                .attemptCount(1)
                .lastError(errorMsg)
                .build();
            log.debug("Created notification failure record for invoice: {}, channel: {}", invoiceId, channel);
            return repository.save(delivery);
        }
    }

    /**
     * Record that notification was sent to DLQ (dead letter queue).
     */
    @Transactional
    public NotificationDelivery recordDLQ(String invoiceId, String channel, String errorMsg) {
        Optional<NotificationDelivery> existing = findByInvoiceIdAndChannel(invoiceId, channel);
        
        if (existing.isPresent()) {
            NotificationDelivery delivery = existing.get();
            delivery.setStatus("DLQ");
            delivery.setAttemptCount(delivery.getAttemptCount() + 1);
            delivery.setLastError(errorMsg);
            log.warn("Updated notification DLQ record for invoice: {}, channel: {}, error: {}", 
                invoiceId, channel, errorMsg);
            return repository.save(delivery);
        } else {
            NotificationDelivery delivery = NotificationDelivery.builder()
                .invoiceId(invoiceId)
                .channel(channel)
                .status("DLQ")
                .attemptCount(1)
                .lastError(errorMsg)
                .build();
            log.warn("Created notification DLQ record for invoice: {}, channel: {}", invoiceId, channel);
            return repository.save(delivery);
        }
    }
}
