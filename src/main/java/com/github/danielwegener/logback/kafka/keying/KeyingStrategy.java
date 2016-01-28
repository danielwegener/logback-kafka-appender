package com.github.danielwegener.logback.kafka.keying;


import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * A strategy that can create byte array key for a given {@link ILoggingEvent}.
 * @since 0.0.1
 */
public interface KeyingStrategy<E> {

    /**
     * creates a byte array key for the given {@link ch.qos.logback.classic.spi.ILoggingEvent}
     * @param e the logging event
     */
    byte[] createKey(E e);

}
