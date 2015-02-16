package com.github.danielwegener.logback.kafka.encoding;

import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Layout;

import java.nio.charset.Charset;

public class PatternLayoutKafkaMessageEncoder extends KafkaMessageEncoderBase<ILoggingEvent> {

    public PatternLayoutKafkaMessageEncoder() {
    }

    public PatternLayoutKafkaMessageEncoder(Layout<ILoggingEvent> layout, Charset charset) {
        this.layout = layout;
        this.charset = charset;
    }

    private Layout<ILoggingEvent> layout;
    private Charset charset;
    private static final Charset UTF8 = Charset.forName("UTF-8");

    @Override
    public void start() {
        if (charset == null) {
            addInfo("No charset specified for PatternLayoutKafkaEncoder. Using default UTF8 encoding.");
            charset = UTF8;
        }
        super.start();
    }

    @Override
    public byte[] doEncode(ILoggingEvent loggingEvent) {
        final String message = layout.doLayout(loggingEvent);
        return message.getBytes(charset);
    }

    public void setLayout(Layout<ILoggingEvent> layout) {
        this.layout = layout;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    public Layout<ILoggingEvent> getLayout() {
        return layout;
    }

    public Charset getCharset() {
        return charset;
    }
}