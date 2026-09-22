package com.notification.invoice.repository;

import com.notification.invoice.model.Invoice;
import com.notification.invoice.model.InvoiceStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;

@Repository
public interface InvoiceRepository extends JpaRepository<Invoice, String> {
    List<Invoice> findByDueDateAndStatus(LocalDate dueDate, InvoiceStatus status);

    List<Invoice> findByDueDateAndStatusAndNotificationScheduledFalse(
            LocalDate dueDate,
            InvoiceStatus status
    );

    @Query(value = """
        SELECT * FROM invoices 
        WHERE due_date = :date 
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
