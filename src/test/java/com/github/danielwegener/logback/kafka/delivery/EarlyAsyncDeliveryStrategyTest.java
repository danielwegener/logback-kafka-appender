package com.github.danielwegener.logback.kafka.delivery;

import ch.qos.logback.core.Context;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.record.Record;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.Map;

import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.*;

public class EarlyAsyncDeliveryStrategyTest {

    private final Context context = mock(Context.class);
    @SuppressWarnings("unchecked")
    private final Producer<String,String> producer = mock((Class<Producer<String,String>>)(Class)Producer.class);
    @SuppressWarnings("unchecked")
    private final FailedDeliveryCallback<String> failedDeliveryCallback = mock((Class<FailedDeliveryCallback<String>>)(Class)FailedDeliveryCallback.class);
    @SuppressWarnings("unchecked")
    private final ProducerSupplier<String, String> producerSupplier = mock(ProducerSupplier.class);
    private EarlyAsyncDeliveryStrategy<String, String, String> unit;

    interface ProducerSupplier<K, V> {
        Producer<K, V> get();
    }
    static <T, K, V> T mockDeliveryStrategyCreateProducer(Class<T> clazz, final ProducerSupplier<K, V> producerSupplier) {
        return mock(clazz, withSettings()
                        .defaultAnswer(new Answer() {
                            @Override
                            public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
                                return invocationOnMock.getMethod().getName().equals("createProducer") ? producerSupplier.get() :
                                        CALLS_REAL_METHODS.answer(invocationOnMock);
                            }
                        }));
    }

    @Test
    public void testStartGetStop() {
        when(producerSupplier.get()).thenReturn(producer);
        unit = mockDeliveryStrategyCreateProducer(EarlyAsyncDeliveryStrategy.class, producerSupplier);

        // Start creates producer
        unit.setContext(context);
        unit.start(mock(Map.class), failedDeliveryCallback);
        verify(unit).createProducer();

        // Get returns producer
        Producer producer = unit.getProducer();
        assertThat(producer, sameInstance(producer));

        // Stop closes producer
        unit.stop();
        verify(producer).close();
    }

    @Test
    public void testSend() {
        when(producerSupplier.get()).thenReturn(producer);
        unit = mockDeliveryStrategyCreateProducer(EarlyAsyncDeliveryStrategy.class, producerSupplier);
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
        unit = mockDeliveryStrategyCreateProducer(EarlyAsyncDeliveryStrategy.class, producerSupplier);
        unit.setContext(context);
        unit.start(mock(Map.class), failedDeliveryCallback);

        // Get returns producer
        unit.send(mock(ProducerRecord.class), "send");
        verify(producer).send(any(ProducerRecord.class), any(Callback.class));
    }

    @Test
    public void testStartFailure() {
        when(producerSupplier.get()).thenThrow(KafkaException.class);
        unit = mockDeliveryStrategyCreateProducer(EarlyAsyncDeliveryStrategy.class, producerSupplier);

        // Start fails to create producer
        try {
            unit.start(mock(Map.class), failedDeliveryCallback);
            fail("Expected exception");
        } catch (KafkaException e) {
        }
        verify(unit).createProducer();

        //verify(context).addError(anyString(), any(IllegalArgumentException.class));

        // Get returns null
        Producer producer = unit.getProducer();
        assertThat(producer, nullValue());

    }
}