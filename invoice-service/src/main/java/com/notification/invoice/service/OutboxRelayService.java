package com.notification.invoice.service;

import com.notification.invoice.model.OutboxEvent;
import com.notification.invoice.model.OutboxEventStatus;
import com.notification.invoice.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxRelayService {
    private final OutboxEventRepository outboxRepo;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Lazy
    @Autowired
    private OutboxRelayService self;

    private static final int BATCH_SIZE = 1000;

    @Scheduled(fixedDelay = 5000)
    public void relayPendingEvents() {
        List<OutboxEvent> batch = fetchBatch();
        if(batch.isEmpty()) return;

        for (OutboxEvent event : batch) {
            try {
                kafkaTemplate.send(event.getTopic(), event.getAggregateId(), event.getPayload())
                        .get(5, TimeUnit.SECONDS);

                self.markAsPublished(event.getEventId());
            } catch (Exception e) {
                log.error("Failed event {}", event.getEventId(), e);
                self.markAsFailed(event.getEventId());
            }
        }
    }

    @Transactional
    public List<OutboxEvent> fetchBatch() {
        return outboxRepo.findBatchForUpdate(OutboxEventStatus.PENDING.name(), BATCH_SIZE);
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markAsPublished(UUID id) {
        outboxRepo.findByEventId(id).ifPresent(e -> {
                e.setStatus(OutboxEventStatus.PUBLISHED);
                e.setPublishedAt(LocalDateTime.now());
        });
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void markAsFailed(UUID id) {
        outboxRepo.findByEventId(id).ifPresent(e -> {
            int retries = e.getRetryCount();
            e.setRetryCount(retries + 1);
            if (e.getRetryCount() >= 5) {
                e.setStatus(OutboxEventStatus.FAILED);
            }
        });
    }
}