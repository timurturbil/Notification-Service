package com.notification.notifier.service;

import com.notification.notifier.channel.NotificationSender;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationChannelService {

    private final List<NotificationSender> senders;

    /**
     * Find the appropriate sender for the given channel.
     *
     * @param channel Channel name (EMAIL, SMS, CALL)
     * @return NotificationSender that supports this channel
     * @throws IllegalArgumentException if no sender found for channel
     */
    public NotificationSender getSenderForChannel(String channel) {
        return senders.stream()
            .filter(sender -> sender.supports(channel))
            .findFirst()
            .orElseThrow(() -> {
                log.error("No NotificationSender found for channel: {}", channel);
                return new IllegalArgumentException("Unsupported channel: " + channel);
            });
    }

    /**
     * Check if channel is supported.
     */
    public boolean isChannelSupported(String channel) {
        return senders.stream().anyMatch(sender -> sender.supports(channel));
    }

    /**
     * Get list of all supported channels.
     */
    public List<String> getSupportedChannels() {
        return senders.stream()
            .flatMap(sender -> List.of("EMAIL", "SMS", "CALL").stream()
                .filter(sender::supports))
            .distinct()
            .toList();
    }
}
