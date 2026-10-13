package com.notification.notifier.model;

public enum NotificationStatus {
    /** Claimed by a consumer that is working on the event right now. */
    PROCESSING,
    /** Successfully delivered. */
    DELIVERED,
    /** Not sent because the invoice was already paid or cancelled. */
    SKIPPED,
    /** A transient failure; the event is still in the retry chain. */
    FAILED,
    /** Retries are exhausted. Worth replaying once the cause (e.g. an outage) is gone. */
    DLQ,
    /** A permanent failure. Replaying fails again until someone fixes the cause. */
    REJECTED,
    REPLAYED
}
