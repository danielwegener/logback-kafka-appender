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

    public static TestKafka createTestKafka(int brokerCount) throws IOException, InterruptedException {
        return createTestKafka(brokerCount, null);
    }

    public static TestKafka createTestKafka(int brokerCount, Properties properties) throws IOException, InterruptedException {
        if (properties == null) properties = new Properties();
        final EmbeddedZookeeper zk = new EmbeddedZookeeper(-1);
        zk.startup();

        final List<Integer> kafkaPorts = new ArrayList<Integer>(brokerCount);
        for (int i=0; i<brokerCount; ++i) {
            kafkaPorts.add(-1);
        }
        final EmbeddedKafkaCluster kafka = new EmbeddedKafkaCluster(zk.getConnection(), properties, kafkaPorts);
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
