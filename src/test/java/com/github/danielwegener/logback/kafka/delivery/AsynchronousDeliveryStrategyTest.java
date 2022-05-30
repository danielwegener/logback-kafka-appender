package com.github.danielwegener.logback.kafka.delivery;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.same;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AsynchronousDeliveryStrategyTest {

    @SuppressWarnings("unchecked")
    private final Producer<String,String> producer = mock((Class<Producer<String,String>>)(Class)Producer.class);
    @SuppressWarnings("unchecked")
    private final FailedDeliveryCallback<String> failedDeliveryCallback = mock((Class<FailedDeliveryCallback<String>>)(Class)FailedDeliveryCallback.class);
    private final AsynchronousDeliveryStrategy unit = new AsynchronousDeliveryStrategy();

    private final TopicPartition topicAndPartition = new TopicPartition("topic", 0);
    private final RecordMetadata recordMetadata = new RecordMetadata(topicAndPartition, 0, 0, System
            .currentTimeMillis(), null, 32, 64);

    @Test
    public void testCallbackWillNotTriggerOnFailedDeliveryOnNoException() {
        final ProducerRecord<String,String> record = new ProducerRecord<String,String>("topic", 0, null, "msg");
        unit.send(producer, record, "msg", failedDeliveryCallback);

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
        unit.send(producer, record, "msg", failedDeliveryCallback);

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

        unit.send(producer, record, "msg", failedDeliveryCallback);

        verify(failedDeliveryCallback).onFailedDelivery(eq("msg"), same(exception));
    }

    @Test
    public void testCallbackWillTriggerOnFailedDeliveryOnAnyError() {
        final Exception exception = new KafkaException("miau");
        final ProducerRecord<String,String> record = new ProducerRecord<String,String>("topic", 0, null, "msg");

        when(producer.send(same(record), any(Callback.class))).thenThrow(exception);

        unit.send(producer, record, "msg", failedDeliveryCallback);

        verify(failedDeliveryCallback).onFailedDelivery(eq("msg"), same(exception));
    }

}
