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

    @PostMapping("/dlq/reprocess")
    public ResponseEntity<Map<String, Object>> reprocessAllDlq() {
        log.info("Manual DLQ reprocess triggered");

        int reprocessed = processingService.reprocessAllDlq();

        return ResponseEntity.ok(Map.of(
                "message", "DLQ reprocess completed",
                "reprocessedCount", reprocessed
        ));
    }
}