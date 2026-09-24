package com.notification.notifier.consumer;

import com.notification.events.InvoiceDueEvent;
import com.notification.notifier.channel.NotificationSendException;
import com.notification.notifier.channel.NotificationSender;
import com.notification.notifier.metrics.NotificationMetricsService;
import com.notification.notifier.service.IdempotencyService;
import com.notification.notifier.service.NotificationChannelService;
import com.notification.notifier.service.NotificationDeliveryService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class InvoiceDueEventListener {

    private final NotificationChannelService channelService;
    private final IdempotencyService idempotencyService;
    private final NotificationDeliveryService deliveryService;
    private final NotificationMetricsService metricsService;

    /**
     * Kafka listener for billing.invoice_due topic with retry logic.
     * Retries with exponential backoff: 2s, 8s, 32s, then DLQ.
     *
     * @param event InvoiceDueEvent to process
     * @param attemptNumber Current attempt number (1-4)
     */
    @RetryableTopic(
        attempts = "4",
        backoff = @Backoff(delay = 2000, multiplier = 4.0),
        autoCreateTopics = "true",
        topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
        include = {Exception.class}
    )
    @KafkaListener(
        topics = "billing.invoice_due",
        groupId = "notification-group"
    )
    public void handleInvoiceDueEvent(
        @Payload InvoiceDueEvent event,
        @Header(name = KafkaHeaders.RECEIVED_TOPIC, required = false) String topic,
        @Header(name = "kafka_receivedPartitionId", required = false) Integer partition,
        @Header(name = "kafka_receivedTimestamp", required = false) Long timestamp) {

        log.info("Received InvoiceDueEvent - invoiceId: {}, userId: {}, channel: {}, dueDate: {} " +
                "(topic: {}, partition: {}, timestamp: {})",
            event.invoiceId(), event.userId(), event.channel(), event.dueDate(), topic, partition, timestamp);

        try {
            // Check for idempotent delivery
            if (idempotencyService.isAlreadySent(event.eventId())) {
                log.warn("Idempotency check failed: Notification already sent for invoice: {}, channel: {}. Skipping.",
                    event.invoiceId(), event.channel());
                metricsService.recordNotificationDuplicate(event.channel());
                return;
            }

            // Validate channel
            if (!channelService.isChannelSupported(event.channel())) {
                String errorMsg = "Unsupported notification channel: " + event.channel();
                log.error(errorMsg);
                deliveryService.recordFailure(event, errorMsg);
                metricsService.recordNotificationFailed(event.channel());
                return;
            }

            // Get appropriate sender for channel
            NotificationSender sender = channelService.getSenderForChannel(event.channel());

            // Send notification
            sender.send(event);

            // Record successful delivery
            deliveryService.recordDelivery(event);
            idempotencyService.markAsSent(event.eventId());
            metricsService.recordNotificationSent(event.channel());

            log.info("Successfully sent {} notification for invoice: {}", event.channel(), event.invoiceId());

        } catch (Exception e) {
            if (e instanceof NotificationSendException) {
                log.error("Failed to send notification for invoice: {}, channel: {}, attempt: {}. Error: {}",
                        event.invoiceId(), event.channel(), event.attempt(), e.getMessage());
            } else {
                log.error("Unexpected error processing InvoiceDueEvent for invoice: {}, channel: {}",
                        event.invoiceId(), event.channel(), e);
            }
            deliveryService.recordFailure(event, e.getMessage());
            metricsService.recordNotificationFailed(event.channel());
            throw new RuntimeException("Notification processing failed", e);
        }
    }

    /**
     * DLT (Dead Letter Topic) handler for messages that fail after all retry attempts.
     * This is invoked when all 4 attempts have been exhausted.
     *
     * @param event InvoiceDueEvent that failed
     */
    @DltHandler
    public void handleDltEvent(@Payload InvoiceDueEvent event) {
        log.error("*** DLT HANDLER *** InvoiceDueEvent sent to DLQ after exhausting retries. " +
                "InvoiceId: {}, UserId: {}, Channel: {}, DueDate: {}, EventId: {}",
            event.invoiceId(), event.userId(), event.channel(), event.dueDate(), event.eventId());

        try {
            // Record in database that this notification failed completely
            deliveryService.recordDLQ(event, "Failed after 4 retry attempts, sent to DLQ");
            metricsService.recordNotificationDLQ(event.channel());

            // TODO: Alert operations team, send to incident management system, etc.
            // This should trigger alerting/monitoring for manual intervention

        } catch (Exception e) {
            log.error("Error handling DLT event for invoice: {}", event.invoiceId(), e);
        }
    }
}
