package com.notification.notifier.repository;

import com.notification.notifier.model.NotificationDelivery;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface NotificationDeliveryRepository extends JpaRepository<NotificationDelivery, UUID> {
    Optional<NotificationDelivery> findByEventId(UUID eventId);

    List<NotificationDelivery> findByStatus(String status);

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
}
