package com.notification.invoice.controller;

import com.notification.enums.InvoiceStatus;
import com.notification.invoice.service.InvoiceService;
import com.notification.records.InvoiceStatusResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/invoices")
@RequiredArgsConstructor
public class InvoiceController {

    private final InvoiceService invoiceService;

    @PostMapping("/due-check")
    public ResponseEntity<Map<String, Object>> triggerDueCheck(
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date) {

        LocalDate targetDate = date != null ? date : LocalDate.now().plusDays(3);
        int count = invoiceService.triggerDueCheck(targetDate);

        return ResponseEntity.ok(Map.of(
                "date", targetDate,
                "scheduledCount", count
        ));
    }

    @GetMapping("/{id}/status")
    public ResponseEntity<InvoiceStatusResponse> getStatus(@PathVariable UUID id) {
        InvoiceStatus status = invoiceService.getStatus(id);
        return ResponseEntity.ok(new InvoiceStatusResponse(id, status));
    }
}