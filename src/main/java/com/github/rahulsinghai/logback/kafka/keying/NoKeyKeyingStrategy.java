package com.github.rahulsinghai.logback.kafka.keying;

/**
 * Evenly distributes all written log messages over all available kafka partitions. This strategy
 * can lead to unexpected read orders on clients.
 *
 * @since 0.0.1
 */
public class NoKeyKeyingStrategy implements KeyingStrategy<Object> {

    @Override
    public byte[] createKey(Object e) {
        return null;
    }
}
