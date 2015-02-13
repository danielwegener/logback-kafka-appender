package com.github.danielwegener.logback.kafka.partitioning;


import ch.qos.logback.classic.spi.ILoggingEvent;

public interface PartitioningStrategy<E>  {
    byte[] createKey(ILoggingEvent E);


    static final class KnownStrategies {
        public static PartitioningStrategy forName(String name) {
            if ("CONTEXT_NAME".equalsIgnoreCase(name)) return new ContextNamePartitioningStrategy();
            else if ("HOSTNAME".equalsIgnoreCase(name)) return new HostNamePartitioningStrategy();
            else if ("LOGGER_NAME".equalsIgnoreCase(name)) return new LoggerNamePartitioningStrategy();
            else if ("ROUND_ROBIN".equalsIgnoreCase(name)) return new RoundRobinPartitioningStrategy();
            else if ("THREAD_NAME".equalsIgnoreCase(name)) return new ThreadNamePartitioningStrategy();
            return null;
        }
    }

}
