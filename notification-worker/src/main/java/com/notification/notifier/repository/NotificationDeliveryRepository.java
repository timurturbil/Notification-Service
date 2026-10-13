package com.notification.notifier.repository;

import com.notification.notifier.model.NotificationDelivery;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface NotificationDeliveryRepository extends JpaRepository<NotificationDelivery, UUID> {
    Optional<NotificationDelivery> findByEventId(UUID eventId);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    Optional<NotificationDelivery> findWithLockByEventId(UUID eventId);

    @Query(value = """
        SELECT * FROM notification_deliveries
        WHERE status = :status
        ORDER BY id ASC
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    List<NotificationDelivery> findBatchForUpdate(
            @Param("status") String status,
            @Param("limit") int limit
    );

    /**
     * Claims an event for processing in one atomic statement on the unique event_id index.
     * The claim succeeds when the event is new, waiting for a retry (FAILED), replayed (REPLAYED),
     * or held by a claim older than staleSeconds (the consumer that took it died).
     *
     * @return 1 if the caller now owns the event, 0 if another consumer owns it or it is finished
     */
    @Modifying
    @Query(value = """
        INSERT INTO notification_deliveries
            (id, invoice_id, event_id, event_payload, channel, status, attempt_count, created_at, updated_at)
        VALUES
            (gen_random_uuid(), :invoiceId, :eventId, NULLIF(:eventPayload, ''), :channel, 'PROCESSING', 0, now(), now())
        ON CONFLICT (event_id) DO UPDATE SET
            status = 'PROCESSING',
            updated_at = now()
        WHERE notification_deliveries.status IN ('FAILED', 'REPLAYED')
           OR (notification_deliveries.status = 'PROCESSING'
               AND notification_deliveries.updated_at < now() - (:staleSeconds * interval '1 second'))
        """, nativeQuery = true)
    int claim(
            @Param("invoiceId") UUID invoiceId,
            @Param("eventId") UUID eventId,
            @Param("eventPayload") String eventPayload,
            @Param("channel") String channel,
            @Param("staleSeconds") int staleSeconds
    );

    /**
     * Writes the outcome of an event: inserts the delivery row, or updates it if the event already has one.
     * A DELIVERED row is never overwritten. Empty strings are stored as NULL.
     */
    @Modifying
    @Query(value = """
        INSERT INTO notification_deliveries
            (id, invoice_id, event_id, event_payload, channel, status, attempt_count, last_error, created_at, updated_at)
        VALUES
            (gen_random_uuid(), :invoiceId, :eventId, NULLIF(:eventPayload, ''), :channel, :status, 1, NULLIF(:lastError, ''), now(), now())
        ON CONFLICT (event_id) DO UPDATE SET
            status = EXCLUDED.status,
            last_error = EXCLUDED.last_error,
            attempt_count = notification_deliveries.attempt_count + 1,
            updated_at = now()
        WHERE notification_deliveries.status <> 'DELIVERED'
        """, nativeQuery = true)
    void upsert(
            @Param("invoiceId") UUID invoiceId,
            @Param("eventId") UUID eventId,
            @Param("eventPayload") String eventPayload,
            @Param("channel") String channel,
            @Param("status") String status,
            @Param("lastError") String lastError
    );
}
