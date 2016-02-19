package com.github.danielwegener.logback.kafka;

import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.spi.AppenderAttachable;
import com.github.danielwegener.logback.kafka.delivery.BlockingDeliveryStrategy;
import com.github.danielwegener.logback.kafka.delivery.DeliveryStrategy;
import com.github.danielwegener.logback.kafka.encoding.KafkaMessageEncoder;
import com.github.danielwegener.logback.kafka.keying.KeyingStrategy;
import com.github.danielwegener.logback.kafka.keying.RoundRobinKeyingStrategy;
import static org.apache.kafka.clients.producer.ProducerConfig.*;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @since 0.0.1
 */
public abstract class KafkaAppenderConfig<E> extends UnsynchronizedAppenderBase<E> implements AppenderAttachable<E> {

    protected String topic = null;

    protected KafkaMessageEncoder<E> encoder = null;
    protected KeyingStrategy<? super E> keyingStrategy = null;
    protected DeliveryStrategy deliveryStrategy;

    public static final Set<String> KNOWN_PRODUCER_CONFIG_KEYS = new HashSet<String>();
    public static final Map<String,String> DEPRECATED_PRODUCER_CONFIG_KEYS = new HashMap<String, String>();
    static {
        KNOWN_PRODUCER_CONFIG_KEYS.add(BOOTSTRAP_SERVERS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(METADATA_FETCH_TIMEOUT_CONFIG);
        DEPRECATED_PRODUCER_CONFIG_KEYS.put(METADATA_FETCH_TIMEOUT_CONFIG, MAX_BLOCK_MS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(METADATA_MAX_AGE_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(BATCH_SIZE_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(BUFFER_MEMORY_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ACKS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(TIMEOUT_CONFIG);
        DEPRECATED_PRODUCER_CONFIG_KEYS.put(METADATA_FETCH_TIMEOUT_CONFIG, REQUEST_TIMEOUT_MS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(LINGER_MS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(CLIENT_ID_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(SEND_BUFFER_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(RECEIVE_BUFFER_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(MAX_REQUEST_SIZE_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(RECONNECT_BACKOFF_MS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(BLOCK_ON_BUFFER_FULL_CONFIG);
        DEPRECATED_PRODUCER_CONFIG_KEYS.put(METADATA_FETCH_TIMEOUT_CONFIG, null);
        KNOWN_PRODUCER_CONFIG_KEYS.add(RETRIES_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(RETRY_BACKOFF_MS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(COMPRESSION_TYPE_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(METRICS_SAMPLE_WINDOW_MS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(METRICS_NUM_SAMPLES_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(METRIC_REPORTER_CLASSES_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        KNOWN_PRODUCER_CONFIG_KEYS.add(KEY_SERIALIZER_CLASS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(VALUE_SERIALIZER_CLASS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(CONNECTIONS_MAX_IDLE_MS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(PARTITIONER_CLASS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(MAX_BLOCK_MS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(REQUEST_TIMEOUT_MS_CONFIG);
    }

    protected Map<String,Object> producerConfig = new HashMap<String, Object>();



    protected boolean checkPrerequisites() {
        boolean errorFree = true;



        if (producerConfig.get(BOOTSTRAP_SERVERS_CONFIG) == null) {
            addError("No \"" + BOOTSTRAP_SERVERS_CONFIG + "\" set for the appender named [\""
                    + name + "\"].");
            errorFree = false;
        }

        if (topic == null) {
            addError("No topic set for the appender named [\"" + name + "\"].");
            errorFree = false;
        }

        if (encoder == null) {
            addError("No encoder set for the appender named [\"" + name + "\"].");
            errorFree = false;
        }

        if (keyingStrategy == null) {
            addInfo("No partitionStrategy set for the appender named [\"" + name + "\"]. Using default RoundRobin strategy.");
            keyingStrategy = new RoundRobinKeyingStrategy();
        }

        if (deliveryStrategy == null) {
            addInfo("No sendStrategy set for the appender named [\""+name+"\"]. Using default Blocking strategy.");
            deliveryStrategy = new BlockingDeliveryStrategy();
        }

        return errorFree;
    }

    public void setEncoder(KafkaMessageEncoder<E> layout) {
        this.encoder = layout;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setKeyingStrategy(KeyingStrategy<? super E> keyingStrategy) {
        this.keyingStrategy = keyingStrategy;
    }

    public void addProducerConfig(String keyValue) {
        String[] split = keyValue.split("=", 2);
        if(split.length == 2)
            addProducerConfigValue(split[0], split[1]);
    }


    public void addProducerConfigValue(String key, Object value) {
        if (!KNOWN_PRODUCER_CONFIG_KEYS.contains(key))
            addWarn("The key \""+key+"\" is not a known kafka producer config key.");

        if (DEPRECATED_PRODUCER_CONFIG_KEYS.containsKey(key)) {
            final StringBuilder deprecationMessage =
                    new StringBuilder("The key \""+key+"\" is deprecated in kafka and may be removed in a future version.");
            if (DEPRECATED_PRODUCER_CONFIG_KEYS.get(key) != null) {
                deprecationMessage.append(" Consider using key \"").append(DEPRECATED_PRODUCER_CONFIG_KEYS.get(key)).append("\" instead.");
            }
            addWarn(deprecationMessage.toString());
        }

        this.producerConfig.put(key,value);
    }

    public Map<String, Object> getProducerConfig() {
        return producerConfig;
    }

    public void setDeliveryStrategy(DeliveryStrategy deliveryStrategy) {
        this.deliveryStrategy = deliveryStrategy;
    }



}
