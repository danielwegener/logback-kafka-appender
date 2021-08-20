package com.github.rahulsinghai.logback.kafka.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

public class TestKafka {

    private final EmbeddedZookeeper zookeeper;
    private final EmbeddedKafkaCluster kafkaCluster;

    TestKafka(EmbeddedZookeeper zookeeper, EmbeddedKafkaCluster kafkaCluster) {
        this.zookeeper = zookeeper;
        this.kafkaCluster = kafkaCluster;
    }

    public static TestKafka createTestKafka(int brokerCount, int partitionCount,
        int replicationFactor) throws IOException, InterruptedException {
        final List<Integer> ports = new ArrayList<Integer>(brokerCount);
        for (int i = 0; i < brokerCount; ++i) {
            ports.add(-1);
        }
        final Map<String, String> properties = new HashMap<>();
        properties.put("num.partitions", Integer.toString(partitionCount));
        properties.put("default.replication.factor", Integer.toString(replicationFactor));

        return createTestKafka(ports, properties);
    }

    public static TestKafka createTestKafka(List<Integer> brokerPorts)
        throws IOException, InterruptedException {
        return createTestKafka(brokerPorts, Collections.<String, String>emptyMap());
    }

    public static TestKafka createTestKafka(List<Integer> brokerPorts,
        Map<String, String> properties)
        throws IOException, InterruptedException {
        if (properties == null) {
            properties = Collections.emptyMap();
        }
        final EmbeddedZookeeper zk = new EmbeddedZookeeper(-1, 100);
        zk.startup();

        final EmbeddedKafkaCluster kafka = new EmbeddedKafkaCluster(zk.getConnection(), properties,
            brokerPorts);
        kafka.startup();
        return new TestKafka(zk, kafka);
    }

    public KafkaConsumer<byte[], byte[]> createClient() {
        return createClient(new HashMap<String, Object>());
    }

    public KafkaConsumer<byte[], byte[]> createClient(Map<String, Object> consumerProperties) {
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, getBrokerList());
        //consumerProperties.put("group.id", "simple-consumer-" + new Random().nextInt());
        consumerProperties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        consumerProperties.put("auto.offset.reset", "earliest");
        consumerProperties.put("key.deserializer", ByteArrayDeserializer.class.getName());
        consumerProperties.put("value.deserializer", ByteArrayDeserializer.class.getName());
        return new KafkaConsumer<>(consumerProperties);
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
