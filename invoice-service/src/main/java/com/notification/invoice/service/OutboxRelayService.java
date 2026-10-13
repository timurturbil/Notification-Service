package com.notification.invoice.service;

import com.notification.invoice.model.OutboxEvent;
import com.notification.invoice.model.OutboxEventStatus;
import com.notification.invoice.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class OutboxRelayService {
    private final OutboxEventRepository outboxRepo;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    private static final int BATCH_SIZE = 500;

    /**
     * Publishes PENDING events to Kafka. The whole run is one transaction, so the row locks
     * taken by FOR UPDATE SKIP LOCKED are held until the batch is done and a second instance
     * skips these rows. Stops at the first failure: unsent rows stay PENDING and are retried
     * on the next run.
     */
    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void relayPendingEvents() {
        List<OutboxEvent> batch = outboxRepo.findBatchForUpdate(OutboxEventStatus.PENDING.name(), BATCH_SIZE);

        for (OutboxEvent event : batch) {
            try {
                kafkaTemplate.send(event.getTopic(), event.getAggregateId(), event.getPayload())
                        .get(5, TimeUnit.SECONDS);
            } catch (Exception e) {
                log.error("Failed event {}, will retry on next run", event.getEventId(), e);
                break;
            }
            event.setStatus(OutboxEventStatus.PUBLISHED);
            event.setPublishedAt(LocalDateTime.now());
        }
    }
}
