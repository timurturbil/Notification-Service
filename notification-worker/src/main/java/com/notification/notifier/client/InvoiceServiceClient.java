package com.notification.notifier.client;

import com.notification.records.InvoiceStatusResponse;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;

import java.util.UUID;

@FeignClient(name = "invoice-service", url = "${invoice-service.url:http://localhost:8081}")
public interface InvoiceServiceClient {
    @GetMapping("/api/invoices/{id}/status")
    InvoiceStatusResponse getStatus(@PathVariable("id") UUID id);
}
