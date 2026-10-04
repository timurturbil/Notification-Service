package com.notification.scheduler.service;

import com.notification.scheduler.client.InvoiceServiceClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.ZoneId;

@Service
@RequiredArgsConstructor
@Slf4j
public class DailyInvoiceCheckScheduler {

    private final InvoiceServiceClient invoiceServiceClient;
    private static final ZoneId TR = ZoneId.of("Europe/Istanbul");

    @Scheduled(cron = "0 0 8 * * *", zone = "Europe/Istanbul")
    @SchedulerLock(
            name = "dailyInvoiceCheck",
            lockAtMostFor = "10m",
            lockAtLeastFor = "1m"
    )
    public void checkDueInvoices() {
        LocalDate targetDate = LocalDate.now(TR).plusDays(3);
        log.info("Starting daily invoice due check for targetDate={}", targetDate);

        try {
            var response = invoiceServiceClient.triggerDueCheck(targetDate);
            log.info("Daily check completed -> date={}, scheduledCount={}",
                    response.date(), response.scheduledCount());
        } catch (Exception e) {
            log.error("Error during daily invoice check for date={}", targetDate, e);
        }
    }
}