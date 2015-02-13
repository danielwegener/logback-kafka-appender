package com.github.danielwegener.logback.kafka.encoding;

public interface KafkaEncoder<E> {

    byte[] doEncode(E loggingEvent);

}
