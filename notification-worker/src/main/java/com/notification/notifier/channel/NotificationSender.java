package com.notification.notifier.channel;

import com.notification.events.InvoiceDueEvent;
import com.notification.notifier.exception.NotificationSendException;

public interface NotificationSender {

    boolean supports(String channel);

    void send(InvoiceDueEvent event) throws NotificationSendException;
}
