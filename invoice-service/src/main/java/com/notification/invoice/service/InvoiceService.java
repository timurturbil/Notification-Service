package com.notification.invoice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.notification.events.InvoiceDueEvent;
import com.notification.invoice.model.InvoiceStatus;
import com.notification.invoice.outbox.OutboxEvent;
import com.notification.invoice.outbox.OutboxEventStatus;
import com.notification.invoice.repository.InvoiceRepository;
import com.notification.invoice.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class InvoiceService {
    private final InvoiceRepository invoiceRepository;
    private final OutboxEventRepository outboxRepository;
    private final ObjectMapper objectMapper;

    @Value("${application.invoice.due-check.batch-size:1000}")
    private int batchSize;

    public int triggerDueCheck(LocalDate date) {
        return processBatch(date);
    }

    @Transactional
    protected int processBatch(LocalDate date) {
        var dueInvoices = invoiceRepository.findBatchForUpdate(
                date, InvoiceStatus.UNPAID.name(), batchSize
        );

        if (dueInvoices.isEmpty()) return 0;

        List<OutboxEvent> outboxEvents = new ArrayList<>(dueInvoices.size());

        for (var inv : dueInvoices) {
            try {
                UUID eventId = UUID.randomUUID();
                var event = new InvoiceDueEvent(
                        eventId,
                        inv.getId(),
                        inv.getUserId(),
                        date,
                        "EMAIL",
                        "invoice_due_template",
                        1
                );

                outboxEvents.add(OutboxEvent.builder()
                        .aggregateId(inv.getId().toString())
                        .eventId(eventId)
                        .topic("billing.invoice_due")
                        .payload(objectMapper.writeValueAsString(event))
                        .status(OutboxEventStatus.PENDING)
                        .retryCount(0)
                        .build());

                inv.setNotificationScheduled(true);
            } catch (Exception e) {
                throw new RuntimeException("Outbox serialize fail", e);
            }
        }

        outboxRepository.saveAll(outboxEvents); // tek batch insert
        invoiceRepository.saveAll(dueInvoices); // tek batch update

        return dueInvoices.size();
    }
}