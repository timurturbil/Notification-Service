package com.notification.invoice.service;

import com.notification.invoice.model.OutboxEventStatus;
import com.notification.invoice.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxReprocessService {
    private final OutboxEventRepository outboxRepo;

    @Transactional
    public int reprocessAllFailed() {
        return outboxRepo.bulkReprocess(OutboxEventStatus.FAILED, OutboxEventStatus.PENDING);
    }
}