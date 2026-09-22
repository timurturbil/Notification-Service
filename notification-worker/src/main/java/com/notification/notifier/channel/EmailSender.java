package com.notification.notifier.channel;

import com.notification.events.InvoiceDueEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
@Component
public class EmailSender implements NotificationSender {

    @Override
    public boolean supports(String channel) {
        return "EMAIL".equalsIgnoreCase(channel);
    }

    @Override
    public void send(InvoiceDueEvent event) throws NotificationSendException {
        try {
            double r = ThreadLocalRandom.current().nextDouble();

            // %5 her zaman patlar -> DLQ'ya düşer
            double permanentFailureRate = 0.05;
            if (r < permanentFailureRate) {
                throw new RuntimeException("CHAOS_PERMANENT - DLQ testi için " + event.invoiceId());
            }

            // %15 transient patlar -> retry'de düzelme ihtimali var
            double failureRate = 0.15;
            if (r < permanentFailureRate + failureRate) {
                throw new RuntimeException("CHAOS_TRANSIENT - retry testi için " + event.invoiceId());
            }

            // arada latency de ekle ki Grafana'da lag gör
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
        // mock
    }
}