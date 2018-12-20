package com.github.danielwegener.logback.kafka.keying;


import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * A strategy that can create byte array key for a given {@link ILoggingEvent}.
 * @since 0.0.1
 */
public interface KeyingStrategy<E> {

    /**
     * creates a generic type key for the given {@link ch.qos.logback.classic.spi.ILoggingEvent}
     * the key could be any type from byte array to string or any object
     * @param e the logging event
     * @return a key
     */
    <T> T createKey(E e);

}
