package com.notification.invoice.service;

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

    private final InvoiceService invoiceService;
    private static final ZoneId TR = ZoneId.of("Europe/Istanbul");

    /**
     * Daily trigger for the due check. The ShedLock lock makes sure that only one
     * invoice-service instance runs it when several are deployed.
     */
    @Scheduled(cron = "${application.due-check.cron}", zone = "Europe/Istanbul")
    @SchedulerLock(
            name = "dailyInvoiceCheck",
            lockAtMostFor = "10m",
            lockAtLeastFor = "1m"
    )
    public void checkDueInvoices() {
        LocalDate targetDate = LocalDate.now(TR).plusDays(3);
        log.info("Starting daily invoice due check for targetDate={}", targetDate);

        try {
            int scheduledCount = invoiceService.triggerDueCheck(targetDate);
            log.info("Daily check completed -> date={}, scheduledCount={}", targetDate, scheduledCount);
        } catch (Exception e) {
            log.error("Error during daily invoice check for date={}", targetDate, e);
        }
    }
}
