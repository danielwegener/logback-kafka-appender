package com.github.danielwegener.logback.kafka;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.status.Status;
import com.github.danielwegener.logback.kafka.delivery.DeliveryStrategy;
import com.github.danielwegener.logback.kafka.encoding.KafkaMessageEncoder;
import com.github.danielwegener.logback.kafka.partitioning.PartitioningStrategy;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class KafkaAppenderBaseTest {

    private final KafkaAppenderBase<ILoggingEvent> unit = new KafkaAppenderBase<ILoggingEvent>();
    private final LoggerContext ctx = new LoggerContext();
    @SuppressWarnings("unchecked")
    private final KafkaMessageEncoder<ILoggingEvent> encoder =  Mockito.mock(KafkaMessageEncoder.class);
    private final PartitioningStrategy partitioningStrategy =  Mockito.mock(PartitioningStrategy.class);
    private final DeliveryStrategy<ILoggingEvent> deliveryStrategy =  Mockito.mock(DeliveryStrategy.class);

    @Before
    public void before() {
        ctx.setName("testctx");
        ctx.start();
        unit.setName("kafkaAppenderBase");
        unit.setEncoder(encoder);
        unit.setTopic("topic");
        unit.addProducerConfig("bootstrap.servers=localhost:1234");
        unit.setPartitioningStrategy(partitioningStrategy);
        unit.setDeliveryStrategy(deliveryStrategy);
    }

    @After
    public void printStatuses() {
        for (Status status : ctx.getStatusManager().getCopyOfStatusList()) {
            System.err.println(status.toString());
        }

    }

    @Test
    public void testPerfectStartAndStop() {
        unit.setContext(ctx);

        unit.start();
        assertTrue("isStarted", unit.isStarted());
        unit.stop();
        assertFalse("isStopped", unit.isStarted());
        assertThat(ctx.getStatusManager().getCopyOfStatusList(), Matchers.empty());
    }

    @Test
    public void testDontStartWithoutTopic() {
        unit.setContext(ctx);
        unit.setTopic(null);
        unit.start();
        assertFalse("isStarted", unit.isStarted());
    }

    @Test
    public void testDontStartWithoutContext() {
        unit.setTopic("topic");
        unit.start();
        assertFalse("isStarted", unit.isStarted());
    }

}