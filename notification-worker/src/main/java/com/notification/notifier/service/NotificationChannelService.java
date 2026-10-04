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

    public NotificationSender getSenderForChannel(String channel) {
        return senders.stream()
            .filter(sender -> sender.supports(channel))
            .findFirst()
            .orElseThrow(() -> {
                log.error("No NotificationSender found for channel: {}", channel);
                return new IllegalArgumentException("Unsupported channel: " + channel);
            });
    }

    public boolean isChannelSupported(String channel) {
        return senders.stream().anyMatch(sender -> sender.supports(channel));
    }
}
