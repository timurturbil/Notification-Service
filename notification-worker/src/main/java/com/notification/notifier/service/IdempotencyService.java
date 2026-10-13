package com.notification.notifier.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Redis fast path for duplicates: remembers finished events for 24 hours so that most duplicates
 * are dropped without touching the database. It is only a cache. The guarantee comes from the
 * claim on the unique event_id index (see NotificationDeliveryService.tryClaim).
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private final StringRedisTemplate stringRedisTemplate;

    private static final String REDIS_KEY_PREFIX = "idemp:";
    private static final long IDEMPOTENCY_TTL_SECONDS = 86400; // 24 hours

    /**
     * @return true if Redis remembers the event as finished; false if not, or if Redis is unavailable
     */
    public boolean isAlreadySent(UUID eventId) {
        try {
            return Boolean.TRUE.equals(stringRedisTemplate.hasKey(buildRedisKey(eventId)));
        } catch (Exception e) {
            log.warn("Idempotency: Redis check failed, relying on the DB claim, eventId: {}", eventId, e);
            return false;
        }
    }

    /**
     * Marks the event as finished with TTL. Call only after the outcome is final.
     */
    public void markAsSent(UUID eventId) {
        stringRedisTemplate.opsForValue().set(buildRedisKey(eventId), "1", IDEMPOTENCY_TTL_SECONDS, TimeUnit.SECONDS);
        log.debug("Idempotency: Marked as sent for eventId: {}", eventId);
    }

    private String buildRedisKey(UUID eventId) {
        return REDIS_KEY_PREFIX + eventId;
    }
}
