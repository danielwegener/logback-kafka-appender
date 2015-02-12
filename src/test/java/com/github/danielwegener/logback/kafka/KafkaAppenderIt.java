package com.github.danielwegener.logback.kafka;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import com.github.danielwegener.logback.kafka.util.TestKafka;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;


public class KafkaAppenderIt {

    private TestKafka kafka;
    private KafkaAppender<ILoggingEvent> unit;

    @Before
    public void beforeLogSystemInit() throws IOException {

        kafka = TestKafka.createTestKafka(1);
        kafka.startup();

        final LoggerContext loggerContext = new LoggerContext();
        loggerContext.putProperty("brokers.list", kafka.getBrokerList());

        unit = new KafkaAppender<ILoggingEvent>();
        unit.setLayout(new PatternLayout());
        unit.setTopic("logs");
        unit.setName("TestKafkaAppender");
        unit.setContext(loggerContext);
    }

    @After
    public void tearDown() {
        kafka.shutdown();
    }

    @Test
    public void testLogging() throws InterruptedException {

        final LoggingEvent loggingEvent = new LoggingEvent("a.b.c.d", null, Level.INFO, "message", null, new Object[0]);
        unit.append(loggingEvent);

    }


}