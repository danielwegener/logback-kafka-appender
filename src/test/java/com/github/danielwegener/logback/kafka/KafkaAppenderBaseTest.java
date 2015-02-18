package com.github.danielwegener.logback.kafka;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.BasicStatusManager;
import ch.qos.logback.core.status.ErrorStatus;
import com.github.danielwegener.logback.kafka.delivery.DeliveryStrategy;
import com.github.danielwegener.logback.kafka.delivery.FailedDeliveryCallback;
import com.github.danielwegener.logback.kafka.encoding.KafkaMessageEncoder;
import com.github.danielwegener.logback.kafka.partitioning.PartitioningStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


public class KafkaAppenderBaseTest {

    private final KafkaAppenderBase<ILoggingEvent> unit = new KafkaAppenderBase<ILoggingEvent>();
    private final LoggerContext ctx = new LoggerContext();
    @SuppressWarnings("unchecked")
    private final KafkaMessageEncoder<ILoggingEvent> encoder =  mock(KafkaMessageEncoder.class);
    private final PartitioningStrategy partitioningStrategy =  mock(PartitioningStrategy.class);
    @SuppressWarnings("unchecked")
    private final DeliveryStrategy deliveryStrategy =  mock(DeliveryStrategy.class);

    @Before
    public void before() {
        ctx.setName("testctx");
        ctx.setStatusManager(new BasicStatusManager());
        unit.setContext(ctx);
        unit.setName("kafkaAppenderBase");
        unit.setEncoder(encoder);
        unit.setTopic("topic");
        unit.addProducerConfig("bootstrap.servers=localhost:1234");
        unit.setPartitioningStrategy(partitioningStrategy);
        unit.setDeliveryStrategy(deliveryStrategy);
        ctx.start();
    }

    @Test
    public void testPerfectStartAndStop() {
        unit.start();
        assertTrue("isStarted", unit.isStarted());
        unit.stop();
        assertFalse("isStopped", unit.isStarted());
        assertThat(ctx.getStatusManager().getCopyOfStatusList(), empty());
    }

    @Test
    public void testDontStartWithoutTopic() {
        unit.setTopic(null);
        unit.start();
        assertFalse("isStarted", unit.isStarted());
        assertThat(ctx.getStatusManager().getCopyOfStatusList(),
                hasItem(new ErrorStatus("No topic set for the appender named [\"kafkaAppenderBase\"].", null)));
    }

    @Test
    public void testDontStartWithoutBoostrapServers() {
        unit.getProducerConfig().clear();
        unit.start();
        assertFalse("isStarted", unit.isStarted());
        assertThat(ctx.getStatusManager().getCopyOfStatusList(),
                hasItem(new ErrorStatus("No \"bootstrap.servers\" set for the appender named [\"kafkaAppenderBase\"].", null)));
    }

    @Test
    public void testDontStartWithoutEncoder() {
        unit.setEncoder(null);
        unit.start();
        assertFalse("isStarted", unit.isStarted());
        assertThat(ctx.getStatusManager().getCopyOfStatusList(),
                hasItem(new ErrorStatus("No encoder set for the appender named [\"kafkaAppenderBase\"].", null)));
    }

    @Test
    public void testAppend() {
        when(encoder.doEncode(any(ILoggingEvent.class))).thenReturn(new byte[]{0x00, 0x00});
        unit.start();
        final LoggingEvent evt = new LoggingEvent("fqcn",ctx.getLogger("logger"), Level.ALL, "message", null, new Object[0]);
        unit.append(evt);
        verify(deliveryStrategy).send(any(KafkaProducer.class), any(ProducerRecord.class), eq(evt), any(FailedDeliveryCallback.class));
    }

}