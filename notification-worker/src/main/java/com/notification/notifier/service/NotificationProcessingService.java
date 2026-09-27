package com.notification.notifier.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.notification.events.InvoiceDueEvent;
import com.notification.notifier.channel.NotificationSendException;
import com.notification.notifier.channel.NotificationSender;
import com.notification.notifier.model.NotificationStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationProcessingService {

    private final NotificationChannelService channelService;
    private final IdempotencyService idempotencyService;
    private final NotificationDeliveryService deliveryService;
    private final NotificationMetricsService metricsService;
    private final ObjectMapper objectMapper;

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

        if (!channelService.isChannelSupported(event.channel())) {
            deliveryService.recordFailure(event, "Unsupported channel");
            metricsService.recordNotificationFailed(event.channel());
            return;
        }

        try {
            NotificationSender sender = channelService.getSenderForChannel(event.channel());
            sender.send(event);

            deliveryService.recordDelivery(event);
            idempotencyService.markAsSent(event.eventId());
            metricsService.recordNotificationSent(event.channel());

        } catch (NotificationSendException e) {
            log.error("Failed to send notification for invoice: {}, channel: {}",
                    event.invoiceId(), event.channel(), e);

            deliveryService.recordFailure(event, e.getMessage());
            metricsService.recordNotificationFailed(event.channel());
            throw new RuntimeException("Failed to send notification", e);
        }
    }

    public int reprocessAllDlq() {
        var dlqList = deliveryService.findByStatus(NotificationStatus.DLQ.name());
        if (dlqList.isEmpty()) return 0;

        int success = 0;
        for (var delivery : dlqList) {
            try {
                InvoiceDueEvent event = objectMapper.readValue(delivery.getEventPayload(), InvoiceDueEvent.class);

                process(event);
                success++;
            } catch (Exception e) {
                log.error("DLQ reprocess failed for deliveryId: {}", delivery.getId(), e);
            }
        }
        return success;
    }

    public int reprocessAllFailed() {
        var failedList = deliveryService.findByStatus(NotificationStatus.FAILED.name());
        if (failedList.isEmpty()) return 0;

        int success = 0;
        for (var delivery : failedList) {
            try {
                InvoiceDueEvent event = objectMapper.readValue(delivery.getEventPayload(), InvoiceDueEvent.class);

                process(event);
                success++;
            } catch (Exception e) {
                log.error("Failed reprocess failed for deliveryId: {}", delivery.getId(), e);
            }
        }
        return success;
    }
}