package com.notification.invoice.repository;

import com.notification.invoice.model.Invoice;
import com.notification.enums.InvoiceStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;
import java.util.UUID;

@Repository
public interface InvoiceRepository extends JpaRepository<Invoice, UUID> {
    List<Invoice> findByDueDateAndStatus(LocalDate dueDate, InvoiceStatus status);

    List<Invoice> findByDueDateAndStatusAndNotificationScheduledFalse(
            LocalDate dueDate,
            InvoiceStatus status
    );

    @Query(value = """
        SELECT * FROM invoices 
        WHERE due_date <= :date
          AND status = :status 
          AND notification_scheduled = false
        ORDER BY id ASC
        LIMIT :limit
        FOR UPDATE SKIP LOCKED
        """, nativeQuery = true)
    List<Invoice> findBatchForUpdate(
            @Param("date") LocalDate date,
            @Param("status") String status,
            @Param("limit") int limit
    );
}
