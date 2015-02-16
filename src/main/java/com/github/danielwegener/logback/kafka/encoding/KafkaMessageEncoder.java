package com.github.danielwegener.logback.kafka.encoding;

public interface KafkaMessageEncoder<E> {

    byte[] doEncode(E loggingEvent);

}
