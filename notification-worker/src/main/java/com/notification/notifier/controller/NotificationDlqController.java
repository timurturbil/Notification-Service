package com.notification.notifier.controller;

import com.notification.notifier.service.NotificationProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.UUID;

@Slf4j
@RestController
@RequestMapping("/api/admin/notifications")
@RequiredArgsConstructor
public class NotificationDlqController {

    private final NotificationProcessingService processingService;

    @PostMapping("/dlq/reprocess")
    public ResponseEntity<Map<String, Object>> reprocessAllDlq() {
        log.info("Manual DLQ reprocess triggered");

        int reprocessed = processingService.reprocessAllDlq();

        return ResponseEntity.ok(Map.of(
                "message", "DLQ reprocess completed",
                "reprocessedCount", reprocessed
        ));
    }

    @PostMapping("/{eventId}/reprocess")
    public ResponseEntity<Map<String, Object>> reprocess(@PathVariable UUID eventId) {
        log.info("Manual reprocess triggered for eventId={}", eventId);

        return switch (processingService.reprocess(eventId)) {
            case REPLAYED -> ResponseEntity.ok(Map.of(
                    "message", "Replay sent",
                    "eventId", eventId
            ));
            case NOT_FOUND -> ResponseEntity.notFound().build();
            case NOT_REPLAYABLE -> ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of(
                    "message", "Only DLQ or REJECTED deliveries can be replayed",
                    "eventId", eventId
            ));
        };
    }
}