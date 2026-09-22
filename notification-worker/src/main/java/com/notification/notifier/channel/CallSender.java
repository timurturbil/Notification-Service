package com.notification.notifier.channel;

import com.notification.events.InvoiceDueEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class CallSender implements NotificationSender {

    @Override
    public boolean supports(String channel) {
        return "CALL".equalsIgnoreCase(channel);
    }

    @Override
    public void send(InvoiceDueEvent event) throws NotificationSendException {
        try {
            log.info("Initiating IVR CALL notification for invoice: {} to user: {} (due: {})",
                event.invoiceId(), event.userId(), event.dueDate());
            
            // Mock implementation - simulate voice call initiation
            simulateCallInitiation(event);
            
            log.debug("IVR CALL notification initiated successfully for invoice: {}", event.invoiceId());
        } catch (Exception e) {
            String errorMsg = "Failed to initiate IVR CALL notification for invoice: " + event.invoiceId();
            log.error(errorMsg, e);
            throw new NotificationSendException(errorMsg, e);
        }
    }

    private void simulateCallInitiation(InvoiceDueEvent event) {
        // Mock IVR call simulation
        String ivyScript = String.format("""
            IVR Script for invoice %s:
            1. Greeting: "Hello, this is an automated call regarding your invoice"
            2. Details: "Invoice number %s is due on %s"
            3. Action: "Press 1 to acknowledge, or stay on the line for assistance"
            4. Callback: "We will attempt to reach you again if needed"
            """, event.invoiceId(), event.invoiceId(), event.dueDate());
        
        log.debug("IVR CALL SCRIPT - {}", ivyScript);
    }
}
