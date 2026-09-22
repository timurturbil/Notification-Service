package com.notification.notifier.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.notification.notifier.service.NotificationReprocessService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/admin/notifications")
@RequiredArgsConstructor
public class NotificationReprocessController {
    private final NotificationReprocessService notificationReprocessService;

    @PostMapping("/reprocess/{invoiceId}")
    public String reprocess(@PathVariable String invoiceId) {
        return notificationReprocessService.reprocess(invoiceId);
    }
}