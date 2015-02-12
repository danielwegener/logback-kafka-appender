package com.github.danielwegener.logback.kafka;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KafkaAppenderIt {

    private Logger log = LoggerFactory.getLogger("IT");

    @Test
    public void testLogging() throws InterruptedException {
        final ch.qos.logback.classic.Logger internalLogger = (ch.qos.logback.classic.Logger) log;


        for (int i = 1; i<=1000; ++i) {
            log.info("FOOOO"+i);
        }

        Thread.sleep(1000);


        for (int i = 1; i<=100000; ++i) {
            log.info("B"+i);
        }

    }

}