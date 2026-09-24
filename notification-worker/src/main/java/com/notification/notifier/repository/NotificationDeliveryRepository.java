package com.notification.notifier.repository;

import com.notification.notifier.model.NotificationDelivery;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface NotificationDeliveryRepository extends JpaRepository<NotificationDelivery, UUID> {
    Optional<NotificationDelivery> findByEventId(UUID eventId);
}
