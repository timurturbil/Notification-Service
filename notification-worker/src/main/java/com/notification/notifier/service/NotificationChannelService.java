package com.notification.notifier.service;

import com.notification.events.InvoiceDueEvent;
import com.notification.notifier.channel.NotificationSender;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class NotificationChannelService {

    private final List<NotificationSender> senders;
    private final String defaultChannel;

    public NotificationChannelService(List<NotificationSender> senders,
                                      @Value("${application.notification.default-channel}") String defaultChannel) {
        this.senders = senders;
        this.defaultChannel = defaultChannel;
    }

    /**
     * Decides how an event is delivered. The event only says that an invoice is due;
     * the channel is this worker's decision. For now every event uses the configured default.
     */
    public String resolveChannel(InvoiceDueEvent event) {
        return defaultChannel;
    }

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
