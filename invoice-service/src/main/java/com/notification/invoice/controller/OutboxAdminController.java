package com.notification.invoice.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.notification.invoice.service.OutboxReprocessService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/admin/outbox")
@RequiredArgsConstructor
public class OutboxAdminController {

    private final OutboxReprocessService outboxReprocessService;

    /**
     * Reprocess a failed/DLQ event by creating a new outbox record
     * with a new eventId based on the old event.
     *
     * @param eventId old failed event ID (from notification_deliveries)
     * @return new event ID that will be published
     */
    @PostMapping("/{eventId}/reprocess")
    public ResponseEntity<Map<String, String>> reprocess(@PathVariable UUID eventId) throws JsonProcessingException {
        UUID newEventId = outboxReprocessService.republish(eventId);

        return ResponseEntity.ok(Map.of(
                "oldEventId", eventId.toString(),
                "newEventId", newEventId.toString(),
                "status", "REPROCESSING_INITIATED"
        ));
    }
}