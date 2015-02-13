package com.github.danielwegener.logback.kafka;

import ch.qos.logback.core.Layout;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.status.ErrorStatus;
import ch.qos.logback.core.status.InfoStatus;
import com.github.danielwegener.logback.kafka.delivery.SendStrategy;
import com.github.danielwegener.logback.kafka.partitioning.PartitioningStrategy;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * @author Daniel Wegener (Holisticon AG)
 */
public abstract class KafkaAppenderConfig<E,K> extends UnsynchronizedAppenderBase<E> {

    private static final Charset UTF8 = Charset.forName("UTF-8");
    protected String topic = null;
    protected Layout<E> layout = null;

    protected Charset charset = null;
    protected PartitioningStrategy<K> partitioningStrategy = null;
    protected SendStrategy<? super E> sendStrategy;

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
            addStatus(new ErrorStatus("No \""+ProducerConfig.BOOTSTRAP_SERVERS_CONFIG+"\" set for the appender named \""
                    + name + "\".", this));
            errorFree = false;
        }

        if (topic == null) {
            addStatus(new ErrorStatus("No topic set for the appender named \"" + name + "\".", this));
            errorFree = false;
        }

        if (layout == null) {
            addStatus(new ErrorStatus("No layout set for the appender named \"" + name + "\".", this));
            errorFree = false;
        }

        if (charset == null) {
            addStatus(new InfoStatus("No charset specified for the appender named \"" + name + "\". Using default UTF8 encoding.", this));
            charset = UTF8;
        }

        if (partitioningStrategy == null) {
            addError("No partitionStrategy set for the appender named \"" + name + "\".");
            errorFree = false;
        }

        if (sendStrategy == null) {
            addInfo("No sendStrategy defined. Using default BLOCKING strategy.");
            sendStrategy = SendStrategy.KnownStrategies.forName("BLOCKING");
        }

        return errorFree;
    }

    /** Sets the charset
     * @see java.nio.charset.Charset#forName(String)  */
    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    /**  */
    public void setLayout(Layout<E> layout) {
        this.layout = layout;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setPartitioningStrategy(PartitioningStrategy<K> partitioningStrategy) {
        this.partitioningStrategy = partitioningStrategy;
    }

    //public void setPartitioningStrategy(String partitioningStrategyName) {
    //    this.partitioningStrategy = (PartitioningStrategy<K>) PartitioningStrategy.KnownStrategies.forName(partitioningStrategyName);
    //}



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

    public void addProducerConfigEntry(KeyValuePair entry) {
        if (!KNOWN_PRODUCER_CONFIG_KEYS.contains(entry.getKey()))
            addWarn("The key \""+entry.getKey()+"\" is now a known kafka producer config key.");
        this.producerConfig.put(entry.getKey(), entry.getValue());
    }

    public void setSendStrategy(SendStrategy<E> sendStrategy) {
        this.sendStrategy = sendStrategy;
    }

    public void setSendStrategy(String sendStrategyName) {
        this.sendStrategy = SendStrategy.KnownStrategies.forName(sendStrategyName);
    }


}
