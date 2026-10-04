package com.notification.notifier.exception;

public class PermanentNotificationException extends NotificationSendException {
    public PermanentNotificationException(String message) {
        super(message);
    }
    public PermanentNotificationException(String message, Throwable cause) {
        super(message, cause);
    }
    @Override
    public boolean isTransient() {
        return false;
    }
}