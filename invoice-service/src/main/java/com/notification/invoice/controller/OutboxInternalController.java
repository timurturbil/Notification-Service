package com.notification.invoice.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.notification.invoice.service.OutboxReprocessService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/internal/outbox")
@RequiredArgsConstructor
public class OutboxInternalController {

    private final OutboxReprocessService outboxReprocessService;

    @PostMapping("/{invoiceId}/republish")
    public String reprocess(@PathVariable String invoiceId) throws JsonProcessingException {
        return outboxReprocessService.republish(invoiceId);
    }
}