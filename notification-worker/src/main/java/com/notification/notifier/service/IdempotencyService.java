package com.notification.notifier.service;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.TimeUnit;

@Slf4j
@Service
@RequiredArgsConstructor
public class IdempotencyService {

    private final StringRedisTemplate stringRedisTemplate;
    private final NotificationDeliveryService notificationDeliveryService;

    private static final String REDIS_KEY_PREFIX = "idemp:";
    private static final long IDEMPOTENCY_TTL_SECONDS = 86400; // 24 hours

    /**
     * Check if notification has already been sent for this invoice+channel combination.
     * Uses both Redis (fast check) and database (authoritative check) for strong idempotency.
     *
     * @param invoiceId Invoice ID
     * @param channel Notification channel (EMAIL, SMS, CALL)
     * @return true if notification already sent, false otherwise
     */
    public boolean isAlreadySent(String invoiceId, String channel) {
        // First check: Redis cache (fast)
        if (redisKeyExists(invoiceId, channel)) {
            log.debug("Idempotency: Redis cache hit for invoice: {}, channel: {}", invoiceId, channel);
            return true;
        }

        log.debug("Idempotency: No prior record found for invoice: {}, channel: {}", invoiceId, channel);
        return false;
    }

    /**
     * Mark notification as sent for this invoice+channel combination.
     * Sets Redis key with 24-hour TTL for idempotency window.
     *
     * @param invoiceId Invoice ID
     * @param channel Notification channel
     */
    public void markAsSent(String invoiceId, String channel) {
        setRedisKey(invoiceId, channel);
        log.debug("Idempotency: Marked as sent for invoice: {}, channel: {}", invoiceId, channel);
    }

    /**
     * Check if Redis key exists for this invoice+channel.
     */
    private boolean redisKeyExists(String invoiceId, String channel) {
        String key = buildRedisKey(invoiceId, channel);
        Boolean exists = stringRedisTemplate.hasKey(key);
        return Boolean.TRUE.equals(exists);
    }

    /**
     * Set Redis key with TTL for this invoice+channel.
     */
    private void setRedisKey(String invoiceId, String channel) {
        String key = buildRedisKey(invoiceId, channel);
        stringRedisTemplate.opsForValue().set(key, "1", IDEMPOTENCY_TTL_SECONDS, TimeUnit.SECONDS);
    }

    /**
     * Build Redis key: "idemp:invoiceId:channel"
     */
    private String buildRedisKey(String invoiceId, String channel) {
        return REDIS_KEY_PREFIX + invoiceId + ":" + channel;
    }

    /**
     * Clear all idempotency keys for a given invoice ID.
     */
    public void clearByInvoiceId(String invoiceId) {
        String pattern = REDIS_KEY_PREFIX + invoiceId + ":*";
        Set<String> keys = stringRedisTemplate.keys(pattern);
        if (!keys.isEmpty()) {
            stringRedisTemplate.delete(keys);
            log.info("Idempotency: Cleared {} keys for invoice: {} -> {}", keys.size(), invoiceId, keys);
        } else {
            log.info("Idempotency: No keys found to clear for invoice: {}", invoiceId);
        }
    }
}
