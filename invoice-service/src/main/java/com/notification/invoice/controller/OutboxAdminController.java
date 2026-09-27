package com.notification.invoice.controller;

import com.notification.invoice.model.OutboxEventStatus;
import com.notification.invoice.service.OutboxReprocessService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/admin/outbox")
@RequiredArgsConstructor
public class OutboxAdminController {

    private final OutboxReprocessService outboxReprocessService;

    /**
     * Finds all outbox events with status FAILED and moves them back to PENDING.
     * The relay will try to publish them to Kafka again.
     * No new eventId is created, the same payload is reused.
     * retryCount is reset to 0 to give the relay a new retry budget.
     */
    @PostMapping("/failed/reprocess")
    public ResponseEntity<Map<String, Object>> reprocessAllFailed() {
        int count = outboxReprocessService.reprocessAllFailed();

        return ResponseEntity.ok(Map.of(
                "reprocessedCount", count,
                "status", OutboxEventStatus.PENDING
        ));
    }
}