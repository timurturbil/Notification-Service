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
import java.util.Optional;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationDeliveryService {

    private final NotificationDeliveryRepository repository;
    private final ObjectMapper objectMapper;

    public Optional<NotificationDelivery> findByEventId(UUID eventId) {
        return repository.findByEventId(eventId);
    }

    @Transactional(readOnly = true)
    public List<NotificationDelivery> findByStatus(String status) {
        return repository.findByStatus(status);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recordDelivery(InvoiceDueEvent event) {
        Optional<NotificationDelivery> existing = findByEventId(event.eventId());

        if (existing.isPresent()) {
            NotificationDelivery delivery = existing.get();
            delivery.setStatus(NotificationStatus.DELIVERED.name());
            delivery.setAttemptCount(delivery.getAttemptCount() + 1);
            delivery.setLastError(null);
            log.debug("Updated notification delivery record for invoice: {}, channel: {}", event.invoiceId(), event.channel());
        } else {
            String eventPayload = toJsonPayload(event);
            NotificationDelivery delivery = NotificationDelivery.builder()
                .invoiceId(event.invoiceId())
                .eventId(event.eventId())
                .eventPayload(eventPayload)
                .channel(event.channel())
                .status(NotificationStatus.DELIVERED.name())
                .attemptCount(1)
                .build();
            log.debug("Created notification delivery record for invoice: {}, channel: {}", event.invoiceId(), event.channel());
            repository.save(delivery);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recordFailure(InvoiceDueEvent event, String errorMsg) {
        Optional<NotificationDelivery> existing = findByEventId(event.eventId());
        
        if (existing.isPresent()) {
            NotificationDelivery delivery = existing.get();
            delivery.setStatus(NotificationStatus.FAILED.name());
            delivery.setAttemptCount(delivery.getAttemptCount() + 1);
            delivery.setLastError(errorMsg);
            log.debug("Updated notification failure record for invoice: {}, channel: {}, error: {}", 
                event.invoiceId(), event.channel(), errorMsg);
        } else {
            String eventPayload = toJsonPayload(event);
            NotificationDelivery delivery = NotificationDelivery.builder()
                .invoiceId(event.invoiceId())
                .eventId(event.eventId())
                .eventPayload(eventPayload)
                .channel(event.channel())
                .status(NotificationStatus.FAILED.name())
                .attemptCount(1)
                .lastError(errorMsg)
                .build();
            log.debug("Created notification failure record for invoice: {}, channel: {}", event.invoiceId(), event.channel());
            repository.save(delivery);
        }
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void recordDLQ(InvoiceDueEvent event, String errorMsg) {
        Optional<NotificationDelivery> existing = findByEventId(event.eventId());

        if (existing.isPresent()) {
            NotificationDelivery delivery = existing.get();
            delivery.setStatus(NotificationStatus.DLQ.name());
            delivery.setAttemptCount(delivery.getAttemptCount() + 1);
            delivery.setLastError(errorMsg);
            log.warn("Updated notification DLQ record for invoice: {}, channel: {}, error: {}", 
                event.invoiceId(), event.channel(), errorMsg);
        } else {
            NotificationDelivery delivery = NotificationDelivery.builder()
                .invoiceId(event.invoiceId())
                .eventId(event.eventId())
                .channel(event.channel())
                .status(NotificationStatus.DLQ.name())
                .attemptCount(1)
                .lastError(errorMsg)
                .build();
            log.warn("Created notification DLQ record for invoice: {}, channel: {}", event.invoiceId(), event.channel());
            repository.save(delivery);
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
