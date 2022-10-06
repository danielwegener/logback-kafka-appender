package com.github.danielwegener.logback.kafka.delivery;

import ch.qos.logback.classic.spi.ILoggingEvent;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Map;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BaseDeliveryStrategyTest {

    @SuppressWarnings("unchecked")
    private final Producer<String,String> producer = mock((Class<Producer<String,String>>)(Class)Producer.class);
    @SuppressWarnings("unchecked")
    private final FailedDeliveryCallback<String> failedDeliveryCallback = mock((Class<FailedDeliveryCallback<String>>)(Class)FailedDeliveryCallback.class);
    private final BaseDeliveryStrategy<String, String, String> unit = new BaseDeliveryStrategy<String, String, String>() {
        @Override
        protected Producer<String, String> getProducer() {
            return producer;
        }

        @Override
        public void send(ProducerRecord<String, String> record, String event) {
            doSend(record, event);
        }

        @Override
        public void stop() {

        }
    };

    private final TopicPartition topicAndPartition = new TopicPartition("topic", 0);
    private final RecordMetadata recordMetadata = new RecordMetadata(topicAndPartition, 0, 0, System
            .currentTimeMillis(), null, 32, 64);

    @Before
    public void setUp() {
        unit.start(mock(Map.class), failedDeliveryCallback);
    }

    @Test
    public void testCallbackWillNotTriggerOnFailedDeliveryOnNoException() {
        final ProducerRecord<String,String> record = new ProducerRecord<String,String>("topic", 0, null, "msg");
        unit.send(record, "msg");

        final ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(producer).send(Mockito.refEq(record), callbackCaptor.capture());

        final Callback callback = callbackCaptor.getValue();
        callback.onCompletion(recordMetadata, null);

        verify(failedDeliveryCallback, never()).onFailedDelivery(anyString(), any(Throwable.class));
    }

    @Test
    public void testCallbackWillTriggerOnFailedDeliveryOnException() {
        final IOException exception = new IOException("KABOOM");
        final ProducerRecord<String,String> record = new ProducerRecord<String,String>("topic", 0, null, "msg");
        unit.send(record, "msg");

        final ArgumentCaptor<Callback> callbackCaptor = ArgumentCaptor.forClass(Callback.class);
        verify(producer).send(Mockito.refEq(record), callbackCaptor.capture());

        final Callback callback = callbackCaptor.getValue();
        callback.onCompletion(recordMetadata, exception);

        verify(failedDeliveryCallback).onFailedDelivery("msg", exception);
    }

    @Test
    public void testCallbackWillTriggerOnFailedDeliveryOnProducerSendTimeout() {
        final TimeoutException exception = new TimeoutException("miau");
        final ProducerRecord<String,String> record = new ProducerRecord<String,String>("topic", 0, null, "msg");

        when(producer.send(same(record), any(Callback.class))).thenThrow(exception);

        unit.send(record, "msg");

        verify(failedDeliveryCallback).onFailedDelivery(eq("msg"), same(exception));
    }

}
