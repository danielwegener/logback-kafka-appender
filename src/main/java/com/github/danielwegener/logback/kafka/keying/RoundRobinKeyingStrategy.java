package com.github.danielwegener.logback.kafka.keying;


import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Evenly distributes all written log messages over all available kafka partitions.
 * This strategy can lead to unexpected read orders on clients.
 */
public class RoundRobinKeyingStrategy implements KeyingStrategy {

    @Override
    public byte[] createKey(ILoggingEvent e) {
        return null;
    }
}
