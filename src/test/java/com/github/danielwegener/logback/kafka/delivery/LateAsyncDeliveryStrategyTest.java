package com.github.danielwegener.logback.kafka.delivery;

import ch.qos.logback.core.Context;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TimeoutException;
import org.junit.Test;

import java.util.Map;

import static com.github.danielwegener.logback.kafka.delivery.EarlyAsyncDeliveryStrategyTest.mockDeliveryStrategyCreateProducer;
import com.github.danielwegener.logback.kafka.delivery.EarlyAsyncDeliveryStrategyTest.ProducerSupplier;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.*;

public class LateAsyncDeliveryStrategyTest {

    private final Context context = mock(Context.class);
    @SuppressWarnings("unchecked")
    private final Producer<String,String> producer = mock((Class<Producer<String,String>>)(Class)Producer.class);
    @SuppressWarnings("unchecked")
    private final FailedDeliveryCallback<String> failedDeliveryCallback = mock((Class<FailedDeliveryCallback<String>>)(Class)FailedDeliveryCallback.class);
    @SuppressWarnings("unchecked")
    private final ProducerSupplier<String, String> producerSupplier = mock(ProducerSupplier.class);
    private LateAsyncDeliveryStrategy<String, String, String> unit;

    @Test
    public void testStartGetStop() {
        when(producerSupplier.get()).thenReturn(producer);
        unit = mockDeliveryStrategyCreateProducer(LateAsyncDeliveryStrategy.class, producerSupplier);

        // Start does nothing
        unit.start(mock(Map.class), failedDeliveryCallback);

        // Get initializes and returns producer
        Producer producer = unit.getProducer();
        verify(unit).createProducer();
        assertThat(producer, sameInstance(producer));

        // Stop closes producer
        unit.stop();
        verify(producer).close();
    }

    @Test
    public void testSend() {
        when(producerSupplier.get()).thenReturn(producer);
        unit = mockDeliveryStrategyCreateProducer(LateAsyncDeliveryStrategy.class, producerSupplier);
        unit.setContext(context);
        unit.start(mock(Map.class), failedDeliveryCallback);

        // Get returns producer
        unit.send(mock(ProducerRecord.class), "send");
        verify(producer).send(any(ProducerRecord.class), any(Callback.class));
    }

    @Test
    public void testSendWhenKafkaTimeout() {
        when(producerSupplier.get()).thenReturn(producer);
        when(producer.send(any(ProducerRecord.class), any(Callback.class))).thenThrow(TimeoutException.class);
        unit = mockDeliveryStrategyCreateProducer(LateAsyncDeliveryStrategy.class, producerSupplier);
        unit.setContext(context);
        unit.start(mock(Map.class), failedDeliveryCallback);

        // Get returns producer
        unit.send(mock(ProducerRecord.class), "send");
        verify(producer).send(any(ProducerRecord.class), any(Callback.class));
    }

    @Test
    public void testSendWhenProducerInitFail() {
        when(producerSupplier.get()).thenThrow(KafkaException.class);
        unit = mockDeliveryStrategyCreateProducer(LateAsyncDeliveryStrategy.class, producerSupplier);
        unit.setContext(context);
        unit.start(mock(Map.class), failedDeliveryCallback);

        // Get returns producer
        unit.send(mock(ProducerRecord.class), "send");
        verifyNoMoreInteractions(producer);
    }

    @Test
    public void testGetFailure() {
        when(producerSupplier.get()).thenThrow(KafkaException.class);
        unit = mockDeliveryStrategyCreateProducer(LateAsyncDeliveryStrategy.class, producerSupplier);

        // Start does nothing
        unit.start(mock(Map.class), failedDeliveryCallback);

        // Get initializes and returns producer
        Producer producer = unit.getProducer();
        assertThat(producer, nullValue());
        verify(unit).createProducer();
        //verify(contextAware).addError(anyString(), any(IllegalArgumentException.class));
    }
}