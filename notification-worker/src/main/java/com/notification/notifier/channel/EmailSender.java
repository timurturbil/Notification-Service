package com.notification.notifier.channel;

import com.notification.enums.Channel;
import com.notification.events.InvoiceDueEvent;
import com.notification.notifier.exception.PermanentNotificationException;
import com.notification.notifier.exception.TransientNotificationException;
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
    public void send(InvoiceDueEvent event) {
        injectChaos(event);
        log.info("Sending EMAIL for invoice: {}", event.invoiceId());
        simulateEmailSend(event);
    }

    private void injectChaos(InvoiceDueEvent event) {
        double r = ThreadLocalRandom.current().nextDouble();
        if (r < 0.05) {
            throw new PermanentNotificationException("CHAOS_PERMANENT - for DLQ test " + event.invoiceId());
        }
        if (r < 0.20) {
            throw new TransientNotificationException("CHAOS_TRANSIENT - for retry test " + event.invoiceId());
        }
        if (ThreadLocalRandom.current().nextDouble() < 0.02) {
            sleep(200);
        }
    }

    private void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new TransientNotificationException("Interrupted", e);
        }
    }

    private void simulateEmailSend(InvoiceDueEvent event) {
        // mock email provider call
    }
}