package com.notification.notifier.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.notification.events.InvoiceDueEvent;
import com.notification.notifier.model.NotificationDelivery;
import com.notification.notifier.model.NotificationStatus;
import com.notification.notifier.repository.NotificationDeliveryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationDeliveryService {

    private final NotificationDeliveryRepository repository;
    private final ObjectMapper objectMapper;

    private static final int CLAIM_STALE_SECONDS = 30;

    public Optional<NotificationDelivery> findByEventId(UUID eventId) {
        return repository.findByEventId(eventId);
    }

    public Optional<NotificationDelivery> findByEventIdForUpdate(UUID eventId) {
        return repository.findWithLockByEventId(eventId);
    }

    public List<NotificationDelivery> findBatchForUpdate(String status, int limit) {
        return repository.findBatchForUpdate(status, limit);
    }

    /**
     * Takes ownership of the event before anything is sent. Only one consumer can win,
     * so two copies of the same event never produce two notifications.
     *
     * @return true if the caller may process the event
     */
    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public boolean tryClaim(InvoiceDueEvent event, String channel) {
        return repository.claim(
                event.invoiceId(),
                event.eventId(),
                Objects.toString(toJsonPayload(event), ""),
                channel,
                CLAIM_STALE_SECONDS) == 1;
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recordDelivery(InvoiceDueEvent event, String channel) {
        upsert(event, channel, NotificationStatus.DELIVERED, null);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recordSkipped(InvoiceDueEvent event, String channel, String reason) {
        upsert(event, channel, NotificationStatus.SKIPPED, reason);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recordFailure(InvoiceDueEvent event, String channel, String errorMsg) {
        upsert(event, channel, NotificationStatus.FAILED, errorMsg);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recordDLQ(InvoiceDueEvent event, String channel, String errorMsg) {
        upsert(event, channel, NotificationStatus.DLQ, errorMsg);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recordRejected(InvoiceDueEvent event, String channel, String errorMsg) {
        upsert(event, channel, NotificationStatus.REJECTED, errorMsg);
    }

    private void upsert(InvoiceDueEvent event, String channel, NotificationStatus status, String errorMsg) {
        repository.upsert(
                event.invoiceId(),
                event.eventId(),
                Objects.toString(toJsonPayload(event), ""),
                channel,
                status.name(),
                Objects.toString(errorMsg, ""));
        log.debug("Recorded notification delivery as {} for invoice: {}, channel: {}", status, event.invoiceId(), channel);
    }

    @Transactional
    public void recordReplay(InvoiceDueEvent event) {
        Optional<NotificationDelivery> existing = findByEventId(event.eventId());

        if (existing.isPresent()) {
            NotificationDelivery delivery = existing.get();
            delivery.setStatus(NotificationStatus.REPLAYED.name());
            delivery.setAttemptCount(0);
            log.info("Updated notification replay record for invoice: {}", event.invoiceId());
        } else {
            log.warn("Attempted to replay notification for unknown invoice: {}", event.invoiceId());
        }
    }

    private String toJsonPayload(InvoiceDueEvent event) {
        try {
            return objectMapper.writeValueAsString(event);
        } catch (JsonProcessingException e) {
            log.error("Failed to serialize event payload for eventId: {}", event.eventId(), e);
            return null;
        }
    }
}
