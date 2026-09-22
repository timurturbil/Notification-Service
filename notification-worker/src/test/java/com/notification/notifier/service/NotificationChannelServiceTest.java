package com.notification.notifier.service;

import com.notification.notifier.channel.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class NotificationChannelServiceTest {

    private NotificationChannelService channelService;
    private List<NotificationSender> senders;

    @BeforeEach
    void setUp() {
        senders = List.of(
            new EmailSender(),
            new SmsSender(),
            new CallSender()
        );
        channelService = new NotificationChannelService(senders);
    }

    @Test
    void testGetSenderForEmailChannel() {
        NotificationSender sender = channelService.getSenderForChannel("EMAIL");
        assertNotNull(sender);
        assertTrue(sender instanceof EmailSender);
    }

    @Test
    void testGetSenderForSmsChannel() {
        NotificationSender sender = channelService.getSenderForChannel("SMS");
        assertNotNull(sender);
        assertTrue(sender instanceof SmsSender);
    }

    @Test
    void testGetSenderForCallChannel() {
        NotificationSender sender = channelService.getSenderForChannel("CALL");
        assertNotNull(sender);
        assertTrue(sender instanceof CallSender);
    }

    @Test
    void testGetSenderForUnsupportedChannelThrowsException() {
        assertThrows(IllegalArgumentException.class, 
            () -> channelService.getSenderForChannel("TELEGRAM"));
    }

    @Test
    void testIsChannelSupported() {
        assertTrue(channelService.isChannelSupported("EMAIL"));
        assertTrue(channelService.isChannelSupported("SMS"));
        assertTrue(channelService.isChannelSupported("CALL"));
        assertFalse(channelService.isChannelSupported("SLACK"));
    }

    @Test
    void testGetSupportedChannels() {
        List<String> channels = channelService.getSupportedChannels();
        assertNotNull(channels);
        assertTrue(channels.contains("EMAIL"));
        assertTrue(channels.contains("SMS"));
        assertTrue(channels.contains("CALL"));
        assertEquals(3, channels.size());
    }
}
