package com.notification.notifier.controller;

import com.notification.notifier.service.NotificationProcessingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/admin/notifications")
@RequiredArgsConstructor
public class NotificationDlqController {

    private final NotificationProcessingService processingService;

    /**
     * POST /api/admin/notifications/dlq/reprocess
     * Retries all notifications in DLQ
     */
    @PostMapping("/dlq/reprocess")
    public ResponseEntity<Map<String, Object>> reprocessAllDlq() {
        log.info("Manual DLQ reprocess triggered");

        int reprocessed = processingService.reprocessAllDlq();

        return ResponseEntity.ok(Map.of(
                "message", "DLQ reprocess completed",
                "reprocessedCount", reprocessed
        ));
    }

    /**
     * POST /api/admin/notifications/failed/reprocess
     * Retries all notifications in FAILED
     */
    @PostMapping("/failed/reprocess")
    public ResponseEntity<Map<String, Object>> reprocessAllFailed() {
        log.info("Manual failed reprocess triggered");

        int reprocessed = processingService.reprocessAllFailed();

        return ResponseEntity.ok(Map.of(
                "message", "Failed reprocess completed",
                "reprocessedCount", reprocessed
        ));
    }
}