package com.github.danielwegener.logback.kafka.partitioning;


import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * A strategy that can create byte array key for a given {@link ILoggingEvent}
 */
public interface PartitioningStrategy  {

    /**
     * creates a byte array key for the given {@link ch.qos.logback.classic.spi.ILoggingEvent}
     */
    byte[] createKey(ILoggingEvent E);

}
