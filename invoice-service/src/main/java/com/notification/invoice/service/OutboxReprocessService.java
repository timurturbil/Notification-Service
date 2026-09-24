package com.notification.invoice.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.notification.events.InvoiceDueEvent;
import com.notification.invoice.outbox.OutboxEvent;
import com.notification.invoice.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class OutboxReprocessService {

    private final OutboxEventRepository outboxRepo;
    private final ObjectMapper objectMapper;
    private final jakarta.persistence.EntityManager entityManager;

    @Transactional
    public UUID republish(UUID eventId) throws JsonProcessingException {
        var oldOutbox = outboxRepo.findTopByEventIdOrderByCreatedAtDesc(eventId)
                .orElseThrow(() -> new RuntimeException("Outbox kaydı yok: " + eventId));

        String oldPayload = oldOutbox.getPayload();

        InvoiceDueEvent oldEvent = objectMapper.readValue(oldPayload, InvoiceDueEvent.class);

        UUID newEventId = UUID.randomUUID();
        InvoiceDueEvent newEvent = new InvoiceDueEvent(
                newEventId,
                oldEvent.invoiceId(),
                oldEvent.userId(),
                oldEvent.dueDate(),
                oldEvent.channel(),
                oldEvent.templateId(),
                1
        );

        String newPayload = objectMapper.writeValueAsString(newEvent);

        OutboxEvent newOutbox = OutboxEvent.builder()
                .aggregateId(oldOutbox.getAggregateId())
                .eventId(newEventId)
                .topic(oldOutbox.getTopic())
                .payload(newPayload)
                .status(com.notification.invoice.outbox.OutboxEventStatus.PENDING)
                .createdAt(LocalDateTime.now())
                .build();

        outboxRepo.save(newOutbox);
        return newEventId;
    }
}