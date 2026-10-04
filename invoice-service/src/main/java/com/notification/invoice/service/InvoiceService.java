package com.notification.invoice.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.notification.enums.Channel;
import com.notification.events.InvoiceDueEvent;
import com.notification.enums.InvoiceStatus;
import com.notification.events.InvoiceDueTopics;
import com.notification.invoice.model.OutboxEvent;
import com.notification.invoice.model.OutboxEventStatus;
import com.notification.invoice.repository.InvoiceRepository;
import com.notification.invoice.repository.OutboxEventRepository;
import jakarta.persistence.EntityNotFoundException;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

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
    private final TransactionTemplate transactionTemplate;

    private static final int BATCH_SIZE = 1000;

    public int triggerDueCheck(LocalDate date) {
        int total = 0;
        while (true) {
            Integer processed = transactionTemplate.execute(status -> processBatch(date));
            if (processed == null || processed == 0) break;
            total += processed;
        }
        return total;
    }

    private int processBatch(LocalDate date) {
        var dueInvoices = invoiceRepository.findBatchForUpdate(
                date, InvoiceStatus.UNPAID.name(), BATCH_SIZE
        );

        if (dueInvoices.isEmpty()) return 0;

        List<OutboxEvent> outboxEvents = new ArrayList<>(dueInvoices.size());

        for (var inv : dueInvoices) {
            try {
                UUID eventId = UUID.randomUUID();
                InvoiceDueEvent event = new InvoiceDueEvent(
                        eventId,
                        inv.getId(),
                        inv.getUserId(),
                        inv.getDueDate(),
                        Channel.EMAIL.name(),
                        InvoiceDueTopics.MAIN
                );

                outboxEvents.add(OutboxEvent.builder()
                        .aggregateId(inv.getId().toString())
                        .eventId(eventId)
                        .topic(event.topic())
                        .payload(objectMapper.writeValueAsString(event))
                        .status(OutboxEventStatus.PENDING)
                        .retryCount(0)
                        .build());

                inv.setNotificationScheduled(true);
            } catch (Exception e) {
                throw new RuntimeException("Outbox serialize fail", e);
            }
        }

        outboxRepository.saveAll(outboxEvents);
        invoiceRepository.saveAll(dueInvoices);

        return dueInvoices.size();
    }

    @Transactional(readOnly = true)
    public InvoiceStatus getStatus(UUID invoiceId) {
        return invoiceRepository.findById(invoiceId).
                map(inv -> InvoiceStatus.valueOf(String.valueOf(inv.getStatus()))).
                orElseThrow(() -> new EntityNotFoundException("Invoice not found: " + invoiceId));
    }
}