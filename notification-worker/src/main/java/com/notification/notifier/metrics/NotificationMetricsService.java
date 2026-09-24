package com.notification.notifier.metrics;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationMetricsService {

    private final MeterRegistry meterRegistry;

    private static final String METRIC_SENT = "notifications.sent";
    private static final String METRIC_FAILED = "notifications.failed";
    private static final String METRIC_DLQ = "notifications.dlq";
    private static final String METRIC_DUPLICATE = "notifications.duplicate";

    public void recordNotificationSent(String channel) {
        Counter.builder(METRIC_SENT)
                .description("Total notifications sent successfully")
                .tag("channel", channel)
                .register(meterRegistry)
                .increment();
        log.debug("Metrics: Incremented notifications.sent for channel: {}", channel);
    }

    public void recordNotificationFailed(String channel) {
        Counter.builder(METRIC_FAILED)
                .description("Total notifications failed to send")
                .tag("channel", channel)
                .register(meterRegistry)
                .increment();
        log.debug("Metrics: Incremented notifications.failed for channel: {}", channel);
    }

    public void recordNotificationDLQ(String channel) {
        Counter.builder(METRIC_DLQ)
                .description("Total notifications sent to dead letter queue")
                .tag("channel", channel)
                .register(meterRegistry)
                .increment();
        log.debug("Metrics: Incremented notifications.dlq for channel: {}", channel);
    }

    public void recordNotificationDuplicate(String channel) {
        Counter.builder(METRIC_DUPLICATE)
                .description("Total duplicate notifications skipped due to idempotency")
                .tag("channel", channel)
                .register(meterRegistry)
                .increment();
        log.debug("Metrics: Incremented notifications.duplicate for channel: {}", channel);
    }

    public void recordAttemptCount(String channel, int attemptNumber) {
        io.micrometer.core.instrument.Timer.builder("notifications.attempt")
                .description("Notification attempt tracking")
                .tag("channel", channel)
                .tag("attempt", String.valueOf(attemptNumber))
                .register(meterRegistry)
                .record(() -> { });
    }
}