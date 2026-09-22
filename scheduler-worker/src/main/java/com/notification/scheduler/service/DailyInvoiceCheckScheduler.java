package com.notification.scheduler.service;

import com.notification.scheduler.client.InvoiceServiceClient;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.javacrumbs.shedlock.spring.annotation.SchedulerLock;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.LocalDate;

@Service
@RequiredArgsConstructor
@Slf4j
public class DailyInvoiceCheckScheduler {

    private final InvoiceServiceClient invoiceServiceClient;

    //    @Scheduled(cron = "0 0 2 * * *")
    //    @SchedulerLock(
    //        name = "dailyInvoiceCheck",
    //        lockAtMostFor = "4m59s",
    //        lockAtLeastFor = "5m"
    //    )
    @Scheduled(cron = "*/10 * * * * *")
    @SchedulerLock(name = "dailyInvoiceCheck", lockAtMostFor = "30s", lockAtLeastFor = "5s")
    public void checkDueInvoices() {
        LocalDate targetDate = LocalDate.now().plusDays(3);
        log.info("Starting daily invoice due check for targetDate={}", targetDate);

        try {
            var response = invoiceServiceClient.triggerDueCheck(targetDate);
            log.info("Daily invoice due check completed -> date={}, scheduledCount={}",
                    response.date(), response.scheduledCount());
        } catch (Exception e) {
            log.error("Error during daily invoice check for date={}", targetDate, e);
        }
    }
}