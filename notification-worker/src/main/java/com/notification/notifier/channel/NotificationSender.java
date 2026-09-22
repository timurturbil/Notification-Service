package com.notification.notifier.channel;

import com.notification.events.InvoiceDueEvent;

public interface NotificationSender {

    /**
     * Check if this sender supports the given channel.
     */
    boolean supports(String channel);

    /**
     * Send notification for the given event.
     * 
     * @param event InvoiceDueEvent to send notification for
     * @throws NotificationSendException if sending fails
     */
    void send(InvoiceDueEvent event) throws NotificationSendException;
}
