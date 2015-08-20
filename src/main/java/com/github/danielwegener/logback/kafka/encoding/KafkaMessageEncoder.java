package com.github.danielwegener.logback.kafka.encoding;

/**
 * An Encoder that is able to take an {@code E} and return a {byte[]}.
 * This Encoder should naturally be referential transparent.
 * @param <E> the type of the logging event.
 */
public interface KafkaMessageEncoder<E> {

    /**
     * Encodes a loggingEvent into a byte array.
     * @param loggingEvent the loggingEvent to be encoded.
     */
    byte[] doEncode(E loggingEvent);

}
