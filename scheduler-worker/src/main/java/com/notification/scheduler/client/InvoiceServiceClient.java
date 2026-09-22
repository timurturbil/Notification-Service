package com.notification.scheduler.client;

import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;

import java.time.LocalDate;

@FeignClient(name = "invoice-service", url = "${invoice-service.url:http://localhost:8081}")
public interface InvoiceServiceClient {

    @PostMapping("/api/invoices/due-check")
    DueCheckResponse triggerDueCheck(
            @RequestParam("date") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date
    );

    record DueCheckResponse(LocalDate date, int scheduledCount) {}
}
