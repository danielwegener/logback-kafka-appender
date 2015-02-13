package com.github.danielwegener.logback.kafka.partitioning;


import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Evenly distributes all written log messages over all available kafka partitions.
 * This strategy can lead to unexpected read orders on clients.
 */
public class RoundRobinPartitioningStrategy<T> implements PartitioningStrategy<T> {

    @Override
    public T createKey(ILoggingEvent e) {
        return null;
    }
}
