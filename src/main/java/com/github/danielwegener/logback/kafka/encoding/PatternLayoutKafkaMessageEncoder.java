package com.github.danielwegener.logback.kafka.encoding;

import ch.qos.logback.core.Layout;

import java.nio.charset.Charset;

/**
 * @deprecated Use LayoutKafkaMessageEncoder instead!
 * @since 0.0.1
 */
@Deprecated
public class PatternLayoutKafkaMessageEncoder<E> extends LayoutKafkaMessageEncoder<E> {

    public PatternLayoutKafkaMessageEncoder() {}

    public PatternLayoutKafkaMessageEncoder(Layout<E> layout, Charset charset) {
        super(layout, charset);
    }
}
