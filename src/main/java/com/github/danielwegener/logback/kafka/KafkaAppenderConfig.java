package com.github.danielwegener.logback.kafka;

import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.status.ErrorStatus;
import com.github.danielwegener.logback.kafka.delivery.BlockingDeliveryStrategy;
import com.github.danielwegener.logback.kafka.delivery.DeliveryStrategy;
import com.github.danielwegener.logback.kafka.encoding.KafkaMessageEncoder;
import com.github.danielwegener.logback.kafka.partitioning.PartitioningStrategy;
import com.github.danielwegener.logback.kafka.partitioning.RoundRobinPartitioningStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public abstract class KafkaAppenderConfig<E> extends UnsynchronizedAppenderBase<E> {

    protected String topic = null;

    protected KafkaMessageEncoder<E> encoder = null;
    protected PartitioningStrategy partitioningStrategy = null;
    protected DeliveryStrategy<? super E> deliveryStrategy;

    public static final Set<String> KNOWN_PRODUCER_CONFIG_KEYS = new HashSet<String>();
    static {
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.ACKS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.BATCH_SIZE_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.BLOCK_ON_BUFFER_FULL_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.BUFFER_MEMORY_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.CLIENT_ID_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.COMPRESSION_TYPE_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.LINGER_MS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.MAX_REQUEST_SIZE_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.METADATA_FETCH_TIMEOUT_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.METADATA_MAX_AGE_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.METRICS_NUM_SAMPLES_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.RECEIVE_BUFFER_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.RETRIES_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.RETRY_BACKOFF_MS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.SEND_BUFFER_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.TIMEOUT_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
    }

    protected Map<String,Object> producerConfig = new HashMap<String, Object>();



    protected boolean checkPrerequisites() {
        boolean errorFree = true;



        if (producerConfig.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG) == null) {
            addError("No \"" + ProducerConfig.BOOTSTRAP_SERVERS_CONFIG + "\" set for the appender named [\""
                    + name + "\"].");
            errorFree = false;
        }

        if (topic == null) {
            addStatus(new ErrorStatus("No topic set for the appender named [\"" + name + "\"].", this));
            errorFree = false;
        }

        if (encoder == null) {
            addStatus(new ErrorStatus("No encoder set for the appender named [\"" + name + "\"].", this));
            errorFree = false;
        }

        if (partitioningStrategy == null) {
            addError("No partitionStrategy set for the appender named [\"" + name + "\"]. Using default RoundRobin strategy.");
            partitioningStrategy = new RoundRobinPartitioningStrategy();
        }

        if (deliveryStrategy == null) {
            addInfo("No sendStrategy set for the appender named [\""+name+"\"]. Using default Blocking strategy.");
            deliveryStrategy = new BlockingDeliveryStrategy<E>();
        }

        return errorFree;
    }

    /**  */
    public void setEncoder(KafkaMessageEncoder<E> layout) {
        this.encoder = layout;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setPartitioningStrategy(PartitioningStrategy partitioningStrategy) {
        this.partitioningStrategy = partitioningStrategy;
    }

    public void addProducerConfig(String keyValue) {
        String[] split = keyValue.split("=", 2);
        if(split.length == 2)
            addProducerConfigValue(split[0], split[1]);
    }


    public void addProducerConfigValue(String key, Object value) {
        if (!KNOWN_PRODUCER_CONFIG_KEYS.contains(key))
            addWarn("The key \""+key+"\" is now a known kafka producer config key.");
        this.producerConfig.put(key,value);
    }


    public void setDeliveryStrategy(DeliveryStrategy<E> deliveryStrategy) {
        this.deliveryStrategy = deliveryStrategy;
    }



}
