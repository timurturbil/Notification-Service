package com.notification.invoice.repository;

import com.notification.invoice.model.OutboxEvent;
import com.notification.invoice.model.OutboxEventStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, UUID> {
    @Query(value = """
    SELECT * FROM outbox
    WHERE status = :status
    ORDER BY created_at ASC
    LIMIT :limit
    FOR UPDATE SKIP LOCKED
    """, nativeQuery = true)
    List<OutboxEvent> findBatchForUpdate(
            @Param("status") String status,
            @Param("limit") int limit
    );

    @Modifying
    @Query("UPDATE OutboxEvent o SET o.status = :newStatus, o.retryCount = 0, o.publishedAt = null WHERE o.status = :oldStatus")
    int bulkReprocess(@Param("oldStatus") OutboxEventStatus oldStatus, @Param("newStatus") OutboxEventStatus newStatus);

    Optional<OutboxEvent> findByEventId(UUID eventId);
}
