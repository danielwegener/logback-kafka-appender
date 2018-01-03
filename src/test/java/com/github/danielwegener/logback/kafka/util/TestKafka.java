package com.github.danielwegener.logback.kafka.util;

import java.util.Collections;
import java.util.Map;
import kafka.consumer.Consumer;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.kafka.clients.KafkaClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Random;

/**
 * @author Daniel Wegener (Holisticon AG)
 */
public class TestKafka {

    private final EmbeddedZookeeper zookeeper;
    private final EmbeddedKafkaCluster kafkaCluster;

    public ConsumerConnector createClient() {
        return createClient(new Properties());
    }

    public ConsumerConnector createClient(Properties consumerProperties) {
        consumerProperties.put("metadata.broker.list", getBrokerList());
        consumerProperties.put("group.id", "simple-consumer-" + new Random().nextInt());
        consumerProperties.put("auto.commit.enable","false");
        consumerProperties.put("auto.offset.reset","smallest");
        consumerProperties.put("zookeeper.connect", getZookeeperConnection());
        final kafka.consumer.ConsumerConfig consumerConfig = new kafka.consumer.ConsumerConfig(consumerProperties);
        return Consumer.createJavaConsumerConnector(consumerConfig);
    }


    TestKafka(EmbeddedZookeeper zookeeper, EmbeddedKafkaCluster kafkaCluster) {
        this.zookeeper = zookeeper;
        this.kafkaCluster = kafkaCluster;
    }

    public static TestKafka createTestKafka(int brokerCount) throws IOException, InterruptedException {
        final List<Integer> ports = new ArrayList<Integer>(brokerCount);
        for (int i=0; i<brokerCount; ++i) {
            ports.add(-1);
        }
        return createTestKafka(ports, null);
    }

    public static TestKafka createTestKafka(List<Integer> brokerPorts) throws IOException, InterruptedException {
        return createTestKafka(brokerPorts, null);
    }

    public static TestKafka createTestKafka(List<Integer> brokerPorts, Map<String, String> properties)
            throws IOException, InterruptedException {
        if (properties == null) properties = Collections.emptyMap();
        final EmbeddedZookeeper zk = new EmbeddedZookeeper(-1);
        zk.startup();


        final EmbeddedKafkaCluster kafka = new EmbeddedKafkaCluster(zk.getConnection(), properties, brokerPorts);
        kafka.startup();
        return new TestKafka(zk, kafka);
    }



    public String getZookeeperConnection() {
        return zookeeper.getConnection();
    }

    public String getBrokerList() {
        return kafkaCluster.getBrokerList();
    }

    public void shutdown() {
        try {
            kafkaCluster.shutdown();
        } finally {
            zookeeper.shutdown();
        }
    }

    public void awaitShutdown() {
        kafkaCluster.awaitShutdown();
    }


}
