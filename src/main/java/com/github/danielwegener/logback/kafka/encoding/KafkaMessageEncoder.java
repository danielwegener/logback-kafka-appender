package com.github.danielwegener.logback.kafka.encoding;

/**
 * An Encoder that is able to take an {@code E} and return a {byte[]}.
 * This Encoder should naturally be referential transparent.
 * @param <E>
 */
public interface KafkaMessageEncoder<E> {

    byte[] doEncode(E loggingEvent);

}
