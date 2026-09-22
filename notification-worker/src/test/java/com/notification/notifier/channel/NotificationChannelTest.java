package com.notification.notifier.channel;

import com.notification.events.InvoiceDueEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

class NotificationChannelTest {

    private EmailSender emailSender;
    private SmsSender smsSender;
    private CallSender callSender;
    private InvoiceDueEvent testEvent;

    @BeforeEach
    void setUp() {
        emailSender = new EmailSender();
        smsSender = new SmsSender();
        callSender = new CallSender();
        
        testEvent = new InvoiceDueEvent(
            UUID.randomUUID().toString(),
            "INV-TEST-001",
            "USER-123",
            LocalDate.now().plusDays(3),
            "EMAIL",
            "template-reminder",
            1
        );
    }

    @Test
    void testEmailSenderSupportsEmailChannel() {
        assertTrue(emailSender.supports("EMAIL"));
        assertTrue(emailSender.supports("email"));
        assertFalse(emailSender.supports("SMS"));
    }

    @Test
    void testSmsSenderSupportsSmsChannel() {
        assertTrue(smsSender.supports("SMS"));
        assertTrue(smsSender.supports("sms"));
        assertFalse(smsSender.supports("EMAIL"));
    }

    @Test
    void testCallSenderSupportsCallChannel() {
        assertTrue(callSender.supports("CALL"));
        assertTrue(callSender.supports("call"));
        assertFalse(callSender.supports("EMAIL"));
    }

    @Test
    void testEmailSenderSendNotification() throws NotificationSendException {
        assertDoesNotThrow(() -> emailSender.send(testEvent));
    }

    @Test
    void testSmsSenderSendNotification() throws NotificationSendException {
        assertDoesNotThrow(() -> smsSender.send(testEvent));
    }

    @Test
    void testCallSenderSendNotification() throws NotificationSendException {
        assertDoesNotThrow(() -> callSender.send(testEvent));
    }
}
