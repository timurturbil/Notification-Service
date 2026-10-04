package com.notification.notifier.exception;

public abstract class NotificationSendException extends RuntimeException {
    public NotificationSendException(String message) {
        super(message);
    }
    public NotificationSendException(String message, Throwable cause) {
        super(message, cause);
    }
    public abstract boolean isTransient();
}
