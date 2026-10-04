package com.notification.notifier.service;

import com.notification.notifier.model.NotificationStatus;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private final StringRedisTemplate stringRedisTemplate;
    private final NotificationDeliveryService deliveryService;

    private static final String REDIS_KEY_PREFIX = "idemp:";
    private static final long IDEMPOTENCY_TTL_SECONDS = 86400; // 24 hours

    /**
     * Checks if this event has already been processed.
     * Single source of truth for idempotency is eventId.
     *
     * @param eventId Unique business event ID from outbox
     * @return true if already processed, false otherwise
     */
    public boolean isAlreadySent(UUID eventId) {
        try {
            if (Boolean.TRUE.equals(stringRedisTemplate.hasKey(buildRedisKey(eventId)))) {
                log.debug("Idempotency: Redis cache hit for eventId: {}", eventId);
                return true;
            }
        } catch (Exception e) {
            log.warn("Idempotency: Redis check failed, falling back to DB, eventId: {}", eventId, e);
        }

        boolean delivered = deliveryService.findByEventId(eventId)
                .map(d -> NotificationStatus.DELIVERED.name().equals(d.getStatus()))
                .orElse(false);

        if (delivered) {
            log.debug("Idempotency: DB hit for eventId: {}, re-warming Redis", eventId);
            try {
                markAsSent(eventId);
            } catch (Exception e) {
                log.warn("Idempotency: Redis re-warm failed, eventId: {}", eventId, e);
            }
        }
        return delivered;
    }

    /**
     * Marks the event as processed with TTL.
     * Should be called only after successful delivery.
     *
     * @param eventId Unique business event ID
     */
    public void markAsSent(UUID eventId) {
        String key = buildRedisKey(eventId);
        stringRedisTemplate.opsForValue().set(key, "1", IDEMPOTENCY_TTL_SECONDS, TimeUnit.SECONDS);
        log.debug("Idempotency: Marked as sent for eventId: {}", eventId);
    }


    /**
     * Builds Redis key in format "idemp:{eventId}"
     */
    private String buildRedisKey(UUID eventId) {
        return REDIS_KEY_PREFIX + eventId;
    }
}