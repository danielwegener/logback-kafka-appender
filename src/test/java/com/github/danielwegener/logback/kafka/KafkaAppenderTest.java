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
import com.github.danielwegener.logback.kafka.keying.KeyingStrategy;
import com.github.danielwegener.logback.kafka.producer.ProducerLifeCycleStrategy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class KafkaAppenderTest {

    private final KafkaAppender unit = new KafkaAppender();
    private final LoggerContext ctx = new LoggerContext();
    @SuppressWarnings("unchecked")
    private final KafkaMessageEncoder<ILoggingEvent> encoder =  mock(KafkaMessageEncoder.class);
    @SuppressWarnings("unchecked")
    private final KeyingStrategy<ILoggingEvent> keyingStrategy =  mock(KeyingStrategy.class);
    @SuppressWarnings("unchecked")
    private final DeliveryStrategy deliveryStrategy =  mock(DeliveryStrategy.class);
    @SuppressWarnings("unchecked")
    private final ProducerLifeCycleStrategy<byte[], byte[]> producerLifeCycleStrategy = mock(ProducerLifeCycleStrategy.class);

    @Before
    public void before() {
        ctx.setName("testctx");
        ctx.setStatusManager(new BasicStatusManager());
        unit.setContext(ctx);
        unit.setName("kafkaAppenderBase");
        unit.setEncoder(encoder);
        unit.setTopic("topic");
        unit.addProducerConfig("bootstrap.servers=localhost:1234");
        unit.setKeyingStrategy(keyingStrategy);
        unit.setDeliveryStrategy(deliveryStrategy);
        unit.setProducerLifeCycleStrategy(producerLifeCycleStrategy);
        unit.setDiscardingThreshold(5);
        ctx.start();
    }

    @After
    public void after() {
        ctx.stop();
        unit.stop();
    }

    @Test
    public void testPerfectStartAndStop() {
        unit.start();
        assertTrue("isStarted", unit.isStarted());
        unit.stop();
        assertFalse("isStopped", unit.isStarted());
        assertThat(ctx.getStatusManager().getCopyOfStatusList(), empty());
        verifyZeroInteractions(encoder, keyingStrategy, deliveryStrategy);
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
        whenProducerHasMetadataAvailable();
        unit.start();
        final LoggingEvent evt = new LoggingEvent("fqcn",ctx.getLogger("logger"), Level.ALL, "message", null, new Object[0]);
        unit.append(evt);
        verify(deliveryStrategy).send(any(KafkaProducer.class), any(ProducerRecord.class), eq(evt), any(FailedDeliveryCallback.class));
    }

    @Test
    public void testDeferredAppendForKafkaEvents() {
        when(encoder.doEncode(any(ILoggingEvent.class))).thenReturn(new byte[]{0x00, 0x00});
        whenProducerHasMetadataAvailable();
        unit.start();
        final LoggingEvent deferredEvent = new LoggingEvent("fqcn",ctx.getLogger("org.apache.kafka.clients.logger"), Level.ALL, "deferred message", null, new Object[0]);
        unit.doAppend(deferredEvent);
        verify(deliveryStrategy, times(0)).send(any(KafkaProducer.class), any(ProducerRecord.class), eq(deferredEvent), any(FailedDeliveryCallback.class));
        final LoggingEvent evt = new LoggingEvent("fqcn",ctx.getLogger("logger"), Level.ALL, "message", null, new Object[0]);
        unit.doAppend(evt);
        verify(deliveryStrategy).send(any(KafkaProducer.class), any(ProducerRecord.class), eq(deferredEvent), any(FailedDeliveryCallback.class));
        verify(deliveryStrategy).send(any(KafkaProducer.class), any(ProducerRecord.class), eq(evt), any(FailedDeliveryCallback.class));
    }

    @Test
    public void testDeferredAppendForUnavailableMetadata() {
        when(encoder.doEncode(any(ILoggingEvent.class))).thenReturn(new byte[]{0x00, 0x00});
        whenProducerHasNoMetadataAvailable();
        unit.start();
        final LoggingEvent deferredEvent = new LoggingEvent("fqcn",ctx.getLogger("org.apache.kafka.clients.logger"), Level.ALL, "deferred message", null, new Object[0]);
        unit.doAppend(deferredEvent);
        verify(deliveryStrategy, times(0)).send(any(KafkaProducer.class), any(ProducerRecord.class), eq(deferredEvent), any(FailedDeliveryCallback.class));

        whenProducerHasMetadataAvailable();
        final LoggingEvent evt = new LoggingEvent("fqcn",ctx.getLogger("logger"), Level.ALL, "message", null, new Object[0]);
        unit.doAppend(evt);
        verify(deliveryStrategy).send(any(KafkaProducer.class), any(ProducerRecord.class), eq(deferredEvent), any(FailedDeliveryCallback.class));
        verify(deliveryStrategy).send(any(KafkaProducer.class), any(ProducerRecord.class), eq(evt), any(FailedDeliveryCallback.class));
    }

    private void whenProducerHasPartitions(List<PartitionInfo> partitionInfos) {
        @SuppressWarnings("unchecked")
        Producer<byte[], byte[]> mockProducer = mock(Producer.class);
        when(mockProducer.partitionsFor(anyString())).thenReturn(partitionInfos);
        when(producerLifeCycleStrategy.getProducer()).thenReturn(mockProducer);
    }

    private void whenProducerHasMetadataAvailable() {
        final List<PartitionInfo> nonEmptyList = Collections.singletonList(mock(PartitionInfo.class));
        whenProducerHasPartitions(nonEmptyList);
    }

    private void whenProducerHasNoMetadataAvailable() {
        final List<PartitionInfo> emptyList = Collections.emptyList();
        whenProducerHasPartitions(emptyList);
    }
}
