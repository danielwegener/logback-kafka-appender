package com.github.danielwegener.logback.kafka;

import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;

import java.io.File;
import java.io.IOException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.config.SslConfigs;

import com.github.danielwegener.logback.kafka.delivery.AsynchronousDeliveryStrategy;
import com.github.danielwegener.logback.kafka.delivery.DeliveryStrategy;
import com.github.danielwegener.logback.kafka.keying.KeyingStrategy;
import com.github.danielwegener.logback.kafka.keying.NoKeyKeyingStrategy;
import com.heroku.sdk.EnvKeyStore;

import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.encoder.Encoder;
import ch.qos.logback.core.spi.AppenderAttachable;

/**
 * @since 0.0.1
 */
public abstract class KafkaAppenderConfig<E> extends UnsynchronizedAppenderBase<E> implements AppenderAttachable<E> {

    private static final String KAFKA_TRUSTED_CERT = "KAFKA_TRUSTED_CERT";
    private static final String KAFKA_CLIENT_CERT_KEY = "KAFKA_CLIENT_CERT_KEY";
    private static final String KAFKA_CLIENT_CERT = "KAFKA_CLIENT_CERT";

    protected String topic = null;

    protected Encoder<E> encoder = null;
    protected KeyingStrategy<? super E> keyingStrategy = null;
    protected DeliveryStrategy deliveryStrategy;

    protected Integer partition = null;

    protected boolean appendTimestamp = true;
    protected boolean addSSLProps = false;

    protected Map<String, Object> producerConfig = new HashMap<String, Object>();

    protected boolean checkPrerequisites() {
        boolean errorFree = true;

        if (addSSLProps) {
            errorFree = addSSLProps(this.producerConfig);
        }

        if (producerConfig.get(BOOTSTRAP_SERVERS_CONFIG) == null) {
            addError("No \"" + BOOTSTRAP_SERVERS_CONFIG + "\" set for the appender named [\"" + name + "\"].");
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
            addInfo("No explicit keyingStrategy set for the appender named [\"" + name
                    + "\"]. Using default NoKeyKeyingStrategy.");
            keyingStrategy = new NoKeyKeyingStrategy();
        }

        if (deliveryStrategy == null) {
            addInfo("No explicit deliveryStrategy set for the appender named [\"" + name
                    + "\"]. Using default asynchronous strategy.");
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
        if (split.length == 2) {
            addProducerConfigValue(split[0], split[1]);
        }
    }

    public void addProducerConfigValue(String key, Object value) {
        this.producerConfig.put(key, value);
    }

    public boolean addSSLProps(Map<String, Object> properties) {
        boolean errorFree = true;
        EnvKeyStore envTrustStore;
        try {
            envTrustStore = EnvKeyStore.createWithRandomPassword(KafkaAppenderConfig.KAFKA_TRUSTED_CERT);

            EnvKeyStore envKeyStore = EnvKeyStore.createWithRandomPassword(KafkaAppenderConfig.KAFKA_CLIENT_CERT_KEY,
                    KafkaAppenderConfig.KAFKA_CLIENT_CERT);

            File trustStore = envTrustStore.storeTemp();
            File keyStore = envKeyStore.storeTemp();

            properties.put(SslConfigs.SSL_TRUSTSTORE_TYPE_CONFIG, envTrustStore.type());
            properties.put(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG, trustStore.getAbsolutePath());
            properties.put(SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG, envTrustStore.password());
            properties.put(SslConfigs.SSL_KEYSTORE_TYPE_CONFIG, envKeyStore.type());
            properties.put(SslConfigs.SSL_KEYSTORE_LOCATION_CONFIG, keyStore.getAbsolutePath());
            properties.put(SslConfigs.SSL_KEYSTORE_PASSWORD_CONFIG, envKeyStore.password());
            properties.put("security.protocol", "SSL");
        } catch (CertificateException e) {
            addError("CertificateException occurred for the appender named [\"" + name + "\"].");
            errorFree = false;
        } catch (NoSuchAlgorithmException e) {
            addError("NoSuchAlgorithmException occurred for the appender named [\"" + name + "\"].");
            errorFree = false;
        } catch (KeyStoreException e) {
            addError("KeyStoreException occurred for the appender named [\"" + name + "\"].");
            errorFree = false;
        } catch (IOException e) {
            addError("IOException occurred for the appender named [\"" + name + "\"].");
        }
        return errorFree;
    }

    public Map<String, Object> getProducerConfig() {
        return producerConfig;
    }

    public void setDeliveryStrategy(DeliveryStrategy deliveryStrategy) {
        this.deliveryStrategy = deliveryStrategy;
    }

    public void setPartition(Integer partition) {
        this.partition = partition;
    }

    public boolean isAddSSLProps() {
        return addSSLProps;
    }

    public void setAddSSLProps(boolean addSSLProps) {
        this.addSSLProps = addSSLProps;
    }

    public boolean isAppendTimestamp() {
        return appendTimestamp;
    }

    public void setAppendTimestamp(boolean appendTimestamp) {
        this.appendTimestamp = appendTimestamp;
    }

}
