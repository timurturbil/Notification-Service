package com.notification.notifier.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.notification.events.InvoiceDueEvent;
import com.notification.events.InvoiceDueTopics;
import com.notification.notifier.channel.NotificationSender;
import com.notification.notifier.exception.PermanentNotificationException;
import com.notification.notifier.exception.TransientNotificationException;
import com.notification.notifier.model.NotificationDelivery;
import com.notification.notifier.model.NotificationStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationProcessingService {

    private final NotificationChannelService channelService;
    private final IdempotencyService idempotencyService;
    private final NotificationDeliveryService deliveryService;
    private final NotificationMetricsService metricsService;
    private final InvoiceEligibilityService invoiceEligibilityService;
    private final TransactionTemplate transactionTemplate;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    private static final int BATCH_SIZE = 1000;
    private static final Set<String> REPLAYABLE_STATUSES =
            Set.of(NotificationStatus.DLQ.name(), NotificationStatus.REJECTED.name());

    public void process(InvoiceDueEvent event) {
        String channel = channelService.resolveChannel(event);

        // fast path: Redis remembers finished events for 24 hours
        if (idempotencyService.isAlreadySent(event.eventId())) {
            recordDuplicate(event, channel);
            return;
        }

        // the idempotency guarantee: only one consumer can claim an event
        if (!deliveryService.tryClaim(event, channel)) {
            handleUnclaimed(event, channel);
            return;
        }

        try {
            if (invoiceEligibilityService.shouldSkip(event)) {
                deliveryService.recordSkipped(event, channel, "Invoice already paid or cancelled");
                runSafely("idempotency record", event, () -> idempotencyService.markAsSent(event.eventId()));
                runSafely("metric skipped record", event, () -> metricsService.recordNotificationSkipped(channel));
                return;
            }

            if (!channelService.isChannelSupported(channel)) {
                throw new PermanentNotificationException("Unsupported channel: " + channel);
            }

            NotificationSender sender = channelService.getSenderForChannel(channel);
            sender.send(event);
        } catch (PermanentNotificationException e) {
            log.error("Permanent fail, rejected: {}", event.invoiceId(), e);
            runSafely("delivery rejected record", event, () -> deliveryService.recordRejected(event, channel, e.getMessage()));
            runSafely("metric rejected record", event, () -> metricsService.recordNotificationRejected(channel));
            return;
        } catch (Exception e) {
            log.warn("Transient fail, will retry: {}", event.invoiceId(), e);
            // FAILED also releases the claim, so the retry can take the event again
            runSafely("delivery failure record", event, () -> deliveryService.recordFailure(event, channel, e.getMessage()));
            runSafely("metric failure record", event, () -> metricsService.recordNotificationFailed(channel));
            if (e instanceof TransientNotificationException te) throw te;
            throw new TransientNotificationException(e.getMessage(), e);
        }

        runSafely("delivery record", event, () -> deliveryService.recordDelivery(event, channel));
        runSafely("idempotency record", event, () -> idempotencyService.markAsSent(event.eventId()));
        runSafely("metrics", event, () -> metricsService.recordNotificationSent(channel));
    }

    /**
     * The claim was refused: either the event is finished (a duplicate) or another copy of it
     * is being processed right now. In the second case this copy comes back later through the
     * retry topics, and by then the other copy has finished or its claim has gone stale.
     */
    private void handleUnclaimed(InvoiceDueEvent event, String channel) {
        String status = deliveryService.findByEventId(event.eventId())
                .map(NotificationDelivery::getStatus)
                .orElse(null);

        if (NotificationStatus.PROCESSING.name().equals(status)) {
            throw new TransientNotificationException("Event is being processed by another consumer: " + event.eventId());
        }
        recordDuplicate(event, channel);
    }

    private void recordDuplicate(InvoiceDueEvent event, String channel) {
        log.warn("Duplicate notification for eventId: {}", event.eventId());
        metricsService.recordNotificationDuplicate(channel);
    }

    private void runSafely(String step, InvoiceDueEvent event, Runnable action) {
        try {
            action.run();
        } catch (Exception e) {
            log.error("Side effect '{}' failed, eventId: {}", step, event.eventId(), e);
        }
    }

    /**
     * Replays every DLQ delivery (retries exhausted). REJECTED deliveries are left alone:
     * a permanent failure fails again until its cause is fixed, so those are replayed one by one.
     */
    public int reprocessAllDlq() {
        int total = 0;
        while (true) {
            Integer processed = transactionTemplate.execute(status -> reprocessBatch());
            if (processed == null || processed == 0) break;
            total += processed;
        }
        return total;
    }

    private int reprocessBatch() {
        List<NotificationDelivery> batch = deliveryService.findBatchForUpdate(NotificationStatus.DLQ.name(), BATCH_SIZE);
        if(batch.isEmpty()) return 0;

        int replayed = 0;
        for (NotificationDelivery item : batch) {
            try {
                replay(item);
                replayed++;
            } catch (Exception e) {
                log.error("DLQ replay failed eventId={} invoiceId={}", item.getEventId(), item.getInvoiceId(), e);
            }
        }

        return replayed;
    }

    /**
     * Replays one DLQ or REJECTED delivery. Meant for a REJECTED event whose cause has been fixed.
     */
    public ReplayResult reprocess(UUID eventId) {
        return transactionTemplate.execute(status -> {
            NotificationDelivery item = deliveryService.findByEventIdForUpdate(eventId).orElse(null);
            if (item == null) return ReplayResult.NOT_FOUND;
            if (!REPLAYABLE_STATUSES.contains(item.getStatus())) return ReplayResult.NOT_REPLAYABLE;

            try {
                replay(item);
            } catch (Exception e) {
                throw new IllegalStateException("Replay failed for eventId: " + eventId, e);
            }
            return ReplayResult.REPLAYED;
        });
    }

    private void replay(NotificationDelivery item) throws Exception {
        InvoiceDueEvent event = objectMapper.readValue(item.getEventPayload(), InvoiceDueEvent.class);
        kafkaTemplate.send(InvoiceDueTopics.RETRY_0, item.getInvoiceId().toString(), event)
                .get(5, TimeUnit.SECONDS);
        deliveryService.recordReplay(event);
        log.info("Replay sent eventId={} invoiceId={}", item.getEventId(), item.getInvoiceId());
    }

    public enum ReplayResult {
        REPLAYED,
        NOT_FOUND,
        NOT_REPLAYABLE
    }
}
