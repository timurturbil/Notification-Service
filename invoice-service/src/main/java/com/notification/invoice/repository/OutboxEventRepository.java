package com.notification.invoice.repository;

import com.notification.invoice.outbox.OutboxEvent;
import com.notification.invoice.outbox.OutboxEventStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {
    List<OutboxEvent> findByStatus(OutboxEventStatus status);
    Optional<OutboxEvent> findTopByEventIdOrderByCreatedAtDesc(UUID eventId);
}
