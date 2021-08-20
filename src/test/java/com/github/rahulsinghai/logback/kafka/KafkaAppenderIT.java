package com.github.rahulsinghai.logback.kafka;

import static junit.framework.TestCase.assertEquals;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.encoder.PatternLayoutEncoder;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.Appender;
import ch.qos.logback.core.AppenderBase;
import ch.qos.logback.core.status.Status;
import com.github.rahulsinghai.logback.kafka.delivery.AsynchronousDeliveryStrategy;
import com.github.rahulsinghai.logback.kafka.keying.NoKeyKeyingStrategy;
import com.github.rahulsinghai.logback.kafka.util.TestKafka;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;


public class KafkaAppenderIT {

    private static final Charset UTF8 = StandardCharsets.UTF_8;
    private final List<ILoggingEvent> fallbackLoggingEvents = new ArrayList<>();
    @Rule
    public ErrorCollector collector = new ErrorCollector();
    private final Appender<ILoggingEvent> fallbackAppender = new AppenderBase<ILoggingEvent>() {
        @Override
        protected void append(ILoggingEvent eventObject) {
            collector
                .addError(new IllegalStateException("Logged to fallback appender: " + eventObject));
        }
    };
    private TestKafka kafka;
    private KafkaAppender<ILoggingEvent> unit;
    private LoggerContext loggerContext;

    @Before
    public void beforeLogSystemInit() throws IOException, InterruptedException {

        kafka = TestKafka.createTestKafka(1, 1, 1);

        loggerContext = new LoggerContext();
        loggerContext.putProperty("brokers.list", kafka.getBrokerList());
        loggerContext.getStatusManager().add(status -> {
            if (status.getEffectiveLevel() > Status.INFO) {
                System.err.println(status);
                if (status.getThrowable() != null) {
                    collector.addError(status.getThrowable());
                } else {
                    collector.addError(new RuntimeException(
                        "StatusManager reported warning: " + status));
                }
            } else {
                System.out.println(status);
            }
        });
        loggerContext.putProperty("HOSTNAME", "localhost");

        unit = new KafkaAppender<>();
        final PatternLayoutEncoder patternLayoutEncoder = new PatternLayoutEncoder();
        patternLayoutEncoder.setPattern("%msg");
        patternLayoutEncoder.setContext(loggerContext);
        patternLayoutEncoder.setCharset(StandardCharsets.UTF_8);
        patternLayoutEncoder.start();
        unit.setEncoder(patternLayoutEncoder);
        unit.setTopic("logs");
        unit.setName("TestKafkaAppender");
        unit.setContext(loggerContext);
        unit.setKeyingStrategy(new NoKeyKeyingStrategy());
        unit.setDeliveryStrategy(new AsynchronousDeliveryStrategy());
        unit.addAppender(fallbackAppender);
        unit.addProducerConfigValue(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBrokerList());
        unit.addProducerConfigValue(ProducerConfig.ACKS_CONFIG, "1");
        unit.addProducerConfigValue(ProducerConfig.MAX_BLOCK_MS_CONFIG, "2000");
        unit.addProducerConfigValue(ProducerConfig.LINGER_MS_CONFIG, "100");
        unit.setPartition(0);
        unit.setDeliveryStrategy(new AsynchronousDeliveryStrategy());
        unit.addAppender(new AppenderBase<ILoggingEvent>() {
            @Override
            protected void append(ILoggingEvent eventObject) {
                fallbackLoggingEvents.add(eventObject);
            }
        });
    }

    @After
    public void tearDown() {
        kafka.shutdown();
        kafka.awaitShutdown();
    }

    @Test
    public void testLogging() {

        final int messageCount = 2048;
        final int messageSize = 1024;

        final Logger logger = loggerContext.getLogger("ROOT");

        unit.start();

        assertTrue("appender is started", unit.isStarted());

        final BitSet messages = new BitSet(messageCount);

        for (int i = 0; i < messageCount; ++i) {
            final String prefix = i + ";";
            final StringBuilder sb = new StringBuilder();
            sb.append(prefix);
            byte[] b = new byte[messageSize - prefix.length()];
            ThreadLocalRandom.current().nextBytes(b);
            for (byte bb : b) {
                sb.append((char) bb & 0x7F);
            }

            final LoggingEvent loggingEvent = new LoggingEvent("a.b.c.d", logger, Level.INFO,
                sb.toString(), null, new Object[0]);
            unit.append(loggingEvent);
            messages.set(i);
        }

        unit.stop();
        assertFalse("appender is stopped", unit.isStarted());

        final KafkaConsumer<byte[], byte[]> javaConsumerConnector = kafka.createClient();
        javaConsumerConnector.assign(Collections.singletonList(new TopicPartition("logs", 0)));
        javaConsumerConnector
            .seekToBeginning(Collections.singletonList(new TopicPartition("logs", 0)));
        final long position = javaConsumerConnector.position(new TopicPartition("logs", 0));
        assertEquals(0, position);

        ConsumerRecords<byte[], byte[]> poll = javaConsumerConnector.poll(Duration.ofMillis(10000));
        int readMessages = 0;
        while (!poll.isEmpty()) {
            for (ConsumerRecord<byte[], byte[]> aPoll : poll) {
                byte[] msg = aPoll.value();
                byte[] msgPrefix = new byte[32];
                System.arraycopy(msg, 0, msgPrefix, 0, 32);
                final String messageFromKafka = new String(msgPrefix, UTF8);
                int delimiter = messageFromKafka.indexOf(';');
                final int msgNo = Integer.parseInt(messageFromKafka.substring(0, delimiter));
                messages.set(msgNo, false);
                readMessages++;
            }
            poll = javaConsumerConnector.poll(Duration.ofMillis(1000));
        }

        assertEquals(messageCount, readMessages);
        assertThat(fallbackLoggingEvents, empty());
        assertEquals("all messages should have been read", BitSet.valueOf(new byte[0]), messages);

    }

}
