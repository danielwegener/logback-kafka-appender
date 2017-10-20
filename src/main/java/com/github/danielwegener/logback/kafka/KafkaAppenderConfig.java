package com.github.danielwegener.logback.kafka;

import static org.apache.kafka.clients.producer.ProducerConfig.ACKS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BATCH_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.BUFFER_MEMORY_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.COMPRESSION_TYPE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.INTERCEPTOR_CLASSES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.LINGER_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_BLOCK_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION;
import static org.apache.kafka.clients.producer.ProducerConfig.MAX_REQUEST_SIZE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.METADATA_MAX_AGE_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.METRICS_NUM_SAMPLES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.METRICS_SAMPLE_WINDOW_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.PARTITIONER_CLASS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RECEIVE_BUFFER_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRIES_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.RETRY_BACKOFF_MS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.SEND_BUFFER_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG;

import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.spi.AppenderAttachable;
import com.github.danielwegener.logback.kafka.delivery.AsynchronousDeliveryStrategy;
import com.github.danielwegener.logback.kafka.delivery.DeliveryStrategy;
import com.github.danielwegener.logback.kafka.keying.KeyingStrategy;
import com.github.danielwegener.logback.kafka.keying.RoundRobinKeyingStrategy;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.common.config.SaslConfigs;
import org.apache.kafka.common.config.SslConfigs;

/**
 * @since 0.0.1
 */
public abstract class KafkaAppenderConfig<E> extends UnsynchronizedAppenderBase<E> implements AppenderAttachable<E> {

    protected String topic = null;

    protected Encoder<E> encoder = null;
    protected KeyingStrategy<? super E> keyingStrategy = null;
    protected DeliveryStrategy deliveryStrategy;

    public static final Set<String> KNOWN_PRODUCER_CONFIG_KEYS = new HashSet<String>();
    public static final Map<String,String> DEPRECATED_PRODUCER_CONFIG_KEYS = new HashMap<String, String>();
    static {
        KNOWN_PRODUCER_CONFIG_KEYS.add(BOOTSTRAP_SERVERS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(KEY_SERIALIZER_CLASS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(VALUE_SERIALIZER_CLASS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(ACKS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(BUFFER_MEMORY_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(COMPRESSION_TYPE_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(RETRIES_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(SslConfigs.SSL_KEY_PASSWORD_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(BATCH_SIZE_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(CLIENT_ID_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(CONNECTIONS_MAX_IDLE_MS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(LINGER_MS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(MAX_BLOCK_MS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(MAX_REQUEST_SIZE_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(PARTITIONER_CLASS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(RECEIVE_BUFFER_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(REQUEST_TIMEOUT_MS_CONFIG);

        //KNOWN_PRODUCER_CONFIG_KEYS.add(SaslConfigs.SASL_JAAS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(SaslConfigs.SASL_KERBEROS_SERVICE_NAME);
        KNOWN_PRODUCER_CONFIG_KEYS.add(SaslConfigs.SASL_MECHANISM);
        KNOWN_PRODUCER_CONFIG_KEYS.add(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(SEND_BUFFER_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(SslConfigs.SSL_ENABLED_PROTOCOLS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(SslConfigs.SSL_PROTOCOL_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(SslConfigs.SSL_PROVIDER_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG);
        //KNOWN_PRODUCER_CONFIG_KEYS.add(ENABLE_IDEMPOTENCE_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(INTERCEPTOR_CLASSES_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION);
        KNOWN_PRODUCER_CONFIG_KEYS.add(METADATA_MAX_AGE_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(METRIC_REPORTER_CLASSES_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(METRICS_NUM_SAMPLES_CONFIG);
        //KNOWN_PRODUCER_CONFIG_KEYS.add(METRICS_RECORDING_LEVEL_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(METRICS_SAMPLE_WINDOW_MS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(RECONNECT_BACKOFF_MS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(RETRY_BACKOFF_MS_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(SaslConfigs.SASL_KERBEROS_KINIT_CMD);
        KNOWN_PRODUCER_CONFIG_KEYS.add(SaslConfigs.SASL_KERBEROS_MIN_TIME_BEFORE_RELOGIN);
        KNOWN_PRODUCER_CONFIG_KEYS.add(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_JITTER);
        KNOWN_PRODUCER_CONFIG_KEYS.add(SaslConfigs.SASL_KERBEROS_TICKET_RENEW_WINDOW_FACTOR);
        KNOWN_PRODUCER_CONFIG_KEYS.add(SslConfigs.SSL_CIPHER_SUITES_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(SslConfigs.SSL_ENDPOINT_IDENTIFICATION_ALGORITHM_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(SslConfigs.SSL_KEYMANAGER_ALGORITHM_CONFIG);
        //KNOWN_PRODUCER_CONFIG_KEYS.add(SslConfigs.SSL_SECURE_RANDOM_IMPLEMENTATION_CONFIG);
        KNOWN_PRODUCER_CONFIG_KEYS.add(SslConfigs.SSL_TRUSTMANAGER_ALGORITHM_CONFIG);
        //KNOWN_PRODUCER_CONFIG_KEYS.add(TRANSACTION_TIMEOUT_CONFIG);
        //KNOWN_PRODUCER_CONFIG_KEYS.add(TRANSACTIONAL_ID_CONFIG);
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
            addInfo("No sendStrategy set for the appender named [\""+name+"\"]. Using default asynchronous strategy.");
            deliveryStrategy = new AsynchronousDeliveryStrategy();
        }

        return errorFree;
    }

    public void setEncoder(Encoder<E> encoder) {
        this.encoder = encoder;
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
