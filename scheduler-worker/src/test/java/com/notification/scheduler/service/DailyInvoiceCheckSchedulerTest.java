package com.notification.scheduler.service;

import com.notification.events.InvoiceDueEvent;
import com.notification.scheduler.client.InvoiceServiceClient;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class DailyInvoiceCheckSchedulerTest {

    @Mock
    private InvoiceServiceClient invoiceServiceClient;

    @Mock
    private KafkaTemplate<String, InvoiceDueEvent> kafkaTemplate;

    private DailyInvoiceCheckScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new DailyInvoiceCheckScheduler(invoiceServiceClient, kafkaTemplate);
    }

    @Test
    void testCheckDueInvoices_WithInvoicesFound() {
        // Arrange
        LocalDate targetDate = LocalDate.now().plusDays(3);
        var invoice1 = new InvoiceServiceClient.InvoiceDto(
            "inv-001",
            "user-001",
            new BigDecimal("1000.00"),
            targetDate,
            "UNPAID"
        );
        var invoice2 = new InvoiceServiceClient.InvoiceDto(
            "inv-002",
            "user-002",
            new BigDecimal("2000.00"),
            targetDate,
            "UNPAID"
        );

        when(invoiceServiceClient.getDueInvoices(targetDate))
            .thenReturn(List.of(invoice1, invoice2));

        // Act
        scheduler.checkDueInvoices();

        // Assert
        verify(invoiceServiceClient, times(1)).getDueInvoices(targetDate);
        verify(kafkaTemplate, times(2)).send(
            eq("billing.invoice_due"),
            any(String.class),
            any(InvoiceDueEvent.class)
        );
    }

    @Test
    void testCheckDueInvoices_WithNoInvoices() {
        // Arrange
        LocalDate targetDate = LocalDate.now().plusDays(3);
        when(invoiceServiceClient.getDueInvoices(targetDate))
            .thenReturn(List.of());

        // Act
        scheduler.checkDueInvoices();

        // Assert
        verify(invoiceServiceClient, times(1)).getDueInvoices(targetDate);
        verify(kafkaTemplate, never()).send(any(), any(), any());
    }

    @Test
    void testCheckDueInvoices_WhenClientThrowsException() {
        // Arrange
        LocalDate targetDate = LocalDate.now().plusDays(3);
        when(invoiceServiceClient.getDueInvoices(targetDate))
            .thenThrow(new RuntimeException("Service unavailable"));

        // Act & Assert
        try {
            scheduler.checkDueInvoices();
        } catch (RuntimeException e) {
            assert e.getMessage().contains("Failed to check due invoices");
        }

        verify(kafkaTemplate, never()).send(any(), any(), any());
    }
}
