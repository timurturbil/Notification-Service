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

    /**
     * Core business logic. Handles its own failure recording.
     * Throws to trigger @RetryableTopic retry.
     */
    public void process(InvoiceDueEvent event) {
        if (idempotencyService.isAlreadySent(event.eventId())) {
            log.warn("Duplicate notification for eventId: {}", event.eventId());
            metricsService.recordNotificationDuplicate(event.channel());
            return;
        }

        if (invoiceEligibilityService.shouldSkip(event)) return;

        if (!channelService.isChannelSupported(event.channel())) {
            deliveryService.recordDLQ(event, "Unsupported channel");
            metricsService.recordNotificationDLQ(event.channel());
            return;
        }

        try {
            NotificationSender sender = channelService.getSenderForChannel(event.channel());
            sender.send(event);
        } catch (PermanentNotificationException e) {
            log.error("Permanent fail, DLQ: {}", event.invoiceId(), e);
            runSafely("delivery dlq record", event, () -> deliveryService.recordDLQ(event, e.getMessage()));
            runSafely("metric dlq record", event, () -> metricsService.recordNotificationDLQ(event.channel()));
            return;
        } catch (Exception e) {
            log.warn("Transient fail, will retry: {}", event.invoiceId(), e);
            runSafely("delivery failure record", event, () -> deliveryService.recordFailure(event, e.getMessage()));
            runSafely("metric failure record", event, () -> metricsService.recordNotificationFailed(event.channel()));
            if (e instanceof TransientNotificationException te) throw te;
            throw new TransientNotificationException(e.getMessage(), e);
        }

        runSafely("delivery record", event, () -> deliveryService.recordDelivery(event));
        runSafely("idempotency record", event, () -> idempotencyService.markAsSent(event.eventId()));
        runSafely("metrics", event, () -> metricsService.recordNotificationSent(event.channel()));
    }

    private void runSafely(String step, InvoiceDueEvent event, Runnable action) {
        try {
            action.run();
        } catch (Exception e) {
            log.error("Side effect '{}' failed, eventId: {}", step, event.eventId(), e);
        }
    }

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
                InvoiceDueEvent event = objectMapper.readValue(item.getEventPayload(), InvoiceDueEvent.class);
                kafkaTemplate.send(InvoiceDueTopics.RETRY_0, item.getInvoiceId().toString(), event)
                        .get(5, TimeUnit.SECONDS);
                deliveryService.recordReplay(event);
                replayed++;
                log.info("DLQ replay sent eventId={} invoiceId={}", item.getEventId(), item.getInvoiceId());
            } catch (Exception e) {
                log.error("DLQ replay failed eventId={} invoiceId={}", item.getEventId(), item.getInvoiceId(), e);
            }
        }

        return replayed;
    }
}