package com.notification.notifier.exception;

public class TransientNotificationException extends NotificationSendException {
    public TransientNotificationException(String message) {
        super(message);
    }
    public TransientNotificationException(String message, Throwable cause) {
        super(message, cause);
    }
    @Override
    public boolean isTransient() {
        return true;
    }
}