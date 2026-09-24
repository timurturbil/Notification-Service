package com.notification.notifier.model;

import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Entity
@Table(name = "notification_deliveries", uniqueConstraints = {
    @UniqueConstraint(columnNames = {"invoice_id", "channel"})
})
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class NotificationDelivery {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @Column(nullable = false)
    private UUID invoiceId;

    @Column(nullable = false)
    private UUID eventId;

    @Column(nullable = false, length = 50)
    private String channel;

    @Column(nullable = false, length = 50)
    private String status = "PENDING";

    @Column(nullable = false)
    private Integer attemptCount = 0;

    @Column(length = 1024)
    private String lastError;

    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    @Column(nullable = false)
    private Instant updatedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = Instant.now();
        updatedAt = Instant.now();
    }

    @PreUpdate
    protected void onUpdate() {
        updatedAt = Instant.now();
    }
}
