package com.notification.notifier.service;

import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestClient;

@Service
@RequiredArgsConstructor
public class NotificationReprocessService {

    private final IdempotencyService idempotencyService;
    private final RestClient.Builder restClientBuilder;

    @Value("${invoice-service.url:http://localhost:8081}")
    private String invoiceServiceUrl;

    public String reprocess(String invoiceId) {
        idempotencyService.clearByInvoiceId(invoiceId);

        RestClient restClient = restClientBuilder
                .baseUrl(invoiceServiceUrl)
                .build();

        String newOutboxId = restClient.post()
                .uri("/internal/outbox/{invoiceId}/republish", invoiceId)
                .retrieve()
                .body(String.class);

        return "Redis temizlendi, yeni outbox: " + newOutboxId;
    }
}