package com.notification.notifier.consumer;

import com.notification.events.InvoiceDueEvent;
import com.notification.events.InvoiceDueTopics;
import com.notification.notifier.exception.PermanentNotificationException;
import com.notification.notifier.service.NotificationMetricsService;
import com.notification.notifier.service.NotificationDeliveryService;
import com.notification.notifier.service.NotificationProcessingService;
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
    private final NotificationProcessingService notificationProcessingService;
    private final NotificationDeliveryService deliveryService;
    private final NotificationMetricsService metricsService;

    @RetryableTopic(
            attempts = "4",
            backoff = @Backoff(delay = 2000, multiplier = 4.0),
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            retryTopicSuffix = InvoiceDueTopics.RETRY_SUFFIX,
            dltTopicSuffix = InvoiceDueTopics.DLT_SUFFIX,
            exclude = {PermanentNotificationException.class}
    )
    @KafkaListener(topics = InvoiceDueTopics.MAIN, groupId = InvoiceDueTopics.GROUP_ID)
    public void handleInvoiceDueEvent(@Payload InvoiceDueEvent event) {
        log.info("Received InvoiceDueEvent invoiceId: {}", event.invoiceId());
        notificationProcessingService.process(event); // will throw if fails -> retry
    }

    @DltHandler
    public void handleDltEvent(@Payload InvoiceDueEvent event,
                               @Header(KafkaHeaders.EXCEPTION_MESSAGE) String ex) {
        log.error("DLQ Error invoiceId={} reason={}", event.invoiceId(), ex);
        deliveryService.recordDLQ(event, ex);
        metricsService.recordNotificationDLQ(event.channel());
    }
}
