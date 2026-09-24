package com.notification.notifier.service;

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
        String key = buildRedisKey(eventId);
        Boolean exists = stringRedisTemplate.hasKey(key);
        boolean hit = Boolean.TRUE.equals(exists);

        if (hit) {
            log.debug("Idempotency: Redis cache hit for eventId: {}", eventId);
        }
        return hit;
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
     * Clears idempotency key for a single event.
     * Used during reprocess flow - deletes only the target event's key.
     * No KEYS pattern scan.
     *
     * @param eventId Event ID to clear
     */
    public void clearByEventId(UUID eventId) {
        String key = buildRedisKey(eventId);
        Boolean deleted = stringRedisTemplate.delete(key);
        if (Boolean.TRUE.equals(deleted)) {
            log.info("Idempotency: Cleared key for eventId: {}", eventId);
        } else {
            log.info("Idempotency: No key found to clear for eventId: {}", eventId);
        }
    }

    /**
     * Builds Redis key in format "idemp:{eventId}"
     */
    private String buildRedisKey(UUID eventId) {
        return REDIS_KEY_PREFIX + eventId;
    }
}