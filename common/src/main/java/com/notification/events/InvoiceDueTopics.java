package com.notification.events;

public final class InvoiceDueTopics {

    public static final String MAIN = "invoice-due";
    public static final String RETRY_SUFFIX = "-notification-retry";
    public static final String DLT_SUFFIX = "-notification-dlt";
    public static final String GROUP_ID = "-notification-group";

    public static final String RETRY_0 = MAIN + RETRY_SUFFIX + "-0";

    private InvoiceDueTopics() {}
}