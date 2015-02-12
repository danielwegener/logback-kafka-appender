package com.github.danielwegener.logback.kafka.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author Daniel Wegener (Holisticon AG)
 */
public class TestKafka {

    private final EmbeddedZookeeper zookeeper;
    private final EmbeddedKafkaCluster kafkaCluster;

    TestKafka(EmbeddedZookeeper zookeeper, EmbeddedKafkaCluster kafkaCluster) {
        this.zookeeper = zookeeper;
        this.kafkaCluster = kafkaCluster;
    }

    public static TestKafka createTestKafka(int brokerCount) {
        return createTestKafka(brokerCount, null);
    }

    public static TestKafka createTestKafka(int brokerCount, Properties properties) {
        if (properties == null) properties = new Properties();
        final EmbeddedZookeeper zk = new EmbeddedZookeeper(-1);
        final List<Integer> kafkaPorts = new ArrayList<Integer>(brokerCount);
        for (int i=0; i<brokerCount; ++i) {
            kafkaPorts.add(-1);
        }
        final EmbeddedKafkaCluster kafka = new EmbeddedKafkaCluster(zk.getConnection(), properties, kafkaPorts);
        return new TestKafka(zk, kafka);
    }


    public void startup() throws IOException {
        zookeeper.startup();
        kafkaCluster.startup();
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


}
