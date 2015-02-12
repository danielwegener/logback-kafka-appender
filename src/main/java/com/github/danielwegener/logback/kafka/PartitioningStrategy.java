package com.github.danielwegener.logback.kafka;


public enum PartitioningStrategy {

    /**
     * Evenly distributes all log messages over all available partitions.
     */
    ROUND_ROBIN,

    /**
     * This strategy uses the HOSTNAME to partition the log messages to kafka.
     * This is useful because it ensures that all log messages issued by this host will remain in the correct order for any consumer.
     * But this strategy will lead to uneven log distribution for a small number of hosts (compared to the number of partitions).
     */
    HOSTNAME,

    /**
     * This strategy uses the hostname and the calling thread as partitioning key. This ensures that all messages logged by the
     * same thread will remain in the correct order for any consumer.
     * But this strategy will lead to uneven log distribution for a small number of threads (compared to the number of partitions).
     */
    THREAD,



}
