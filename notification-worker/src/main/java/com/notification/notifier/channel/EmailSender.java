package com.notification.notifier.channel;

import com.notification.enums.Channel;
import com.notification.events.InvoiceDueEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Component
public class EmailSender implements NotificationSender {

    @Override
    public boolean supports(String channel) {
        return Channel.EMAIL.name().equalsIgnoreCase(channel);
    }

    @Override
    public void send(InvoiceDueEvent event) throws NotificationSendException {
        try {
            double r = ThreadLocalRandom.current().nextDouble();

            // 5% permanent failure - always fails and should end up in DLQ
            // Used to test DLT handling and reprocess flow
            double permanentFailureRate = 0.05;
            if (r < permanentFailureRate) {
                throw new RuntimeException("CHAOS_PERMANENT - for DLQ test " + event.invoiceId());
            }

            // 15% transient failure - may succeed on retry
            // Used to test @RetryableTopic backoff and retry mechanism
            double transientFailureRate = 0.15;
            if (r < permanentFailureRate + transientFailureRate) {
                throw new RuntimeException("CHAOS_TRANSIENT - for retry test " + event.invoiceId());
            }

            // 2% artificial latency - to simulate slow provider
            // and observe consumer lag / latency in Grafana
            if (r < 0.02) {
                Thread.sleep(200);
            }

            log.info("Sending EMAIL for invoice: {}", event.invoiceId());
            simulateEmailSend(event);

        } catch (Exception e) {
            throw new NotificationSendException("Failed to send EMAIL for " + event.invoiceId(), e);
        }
    }

    private void simulateEmailSend(InvoiceDueEvent event) {
        // mock email provider call
    }
}