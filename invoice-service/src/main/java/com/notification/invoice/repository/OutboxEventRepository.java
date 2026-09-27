package com.notification.invoice.repository;

import com.notification.invoice.model.OutboxEvent;
import com.notification.invoice.model.OutboxEventStatus;
import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface OutboxEventRepository extends JpaRepository<OutboxEvent, String> {

    List<OutboxEvent> findByStatus(OutboxEventStatus status);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT o FROM OutboxEvent o WHERE o.status = :status")
    List<OutboxEvent> findByStatusForUpdate(@Param("status") OutboxEventStatus status);
}
