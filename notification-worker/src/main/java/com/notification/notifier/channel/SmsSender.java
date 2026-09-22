package com.notification.notifier.channel;

import com.notification.events.InvoiceDueEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class SmsSender implements NotificationSender {

    @Override
    public boolean supports(String channel) {
        return "SMS".equalsIgnoreCase(channel);
    }

    @Override
    public void send(InvoiceDueEvent event) throws NotificationSendException {
        try {
            log.info("Sending SMS notification for invoice: {} to user: {} (due: {})",
                event.invoiceId(), event.userId(), event.dueDate());
            
            // Mock implementation - simulate SMS sending
            simulateSmsSend(event);
            
            log.debug("SMS notification sent successfully for invoice: {}", event.invoiceId());
        } catch (Exception e) {
            String errorMsg = "Failed to send SMS notification for invoice: " + event.invoiceId();
            log.error(errorMsg, e);
            throw new NotificationSendException(errorMsg, e);
        }
    }

    private void simulateSmsSend(InvoiceDueEvent event) {
        // Mock SMS sending logic
        String message = String.format("""
            Invoice %s is due on %s. Please process payment immediately. \
            Contact billing@company.com for assistance.""", 
            event.invoiceId(), event.dueDate());
        
        log.debug("SMS CONTENT - Message: {} (length: {} chars)", message, message.length());
        
        // Simulate SMS character limit (typically 160 chars per segment)
        if (message.length() > 160) {
            log.debug("SMS will be sent as {} segments", (message.length() + 159) / 160);
        }
    }
}
