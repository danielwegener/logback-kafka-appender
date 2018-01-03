package com.github.danielwegener.logback.kafka.util;

import java.util.HashMap;
import java.util.Map;
import kafka.metrics.KafkaMetricsReporter;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.SystemTime$;
import scala.Some;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import scala.collection.immutable.Vector$;

public class EmbeddedKafkaCluster {
    private final List<Integer> ports;
    private final String zkConnection;
    private final Map<String, String> baseProperties;

    private final String brokerList;

    private final List<KafkaServer> brokers;
    private final List<File> logDirs;

    public EmbeddedKafkaCluster(String zkConnection) {
        this(zkConnection, Collections.<String, String>emptyMap());
    }

    public EmbeddedKafkaCluster(String zkConnection, Map<String, String> baseProperties) {
        this(zkConnection, baseProperties, Collections.singletonList(-1));
    }

    public EmbeddedKafkaCluster(String zkConnection, Map<String, String> baseProperties, List<Integer> ports) {
        this.zkConnection = zkConnection;
        this.ports = resolvePorts(ports);
        this.baseProperties = baseProperties;

        this.brokers = new ArrayList<KafkaServer>();
        this.logDirs = new ArrayList<File>();

        this.brokerList = constructBrokerList(this.ports);
    }

    private List<Integer> resolvePorts(List<Integer> ports) {
        List<Integer> resolvedPorts = new ArrayList<Integer>();
        for (Integer port : ports) {
            resolvedPorts.add(resolvePort(port));
        }
        return resolvedPorts;
    }

    private int resolvePort(int port) {
        if (port == -1) {
            return TestUtils.getAvailablePort();
        }
        return port;
    }

    private String constructBrokerList(List<Integer> ports) {
        StringBuilder sb = new StringBuilder();
        for (Integer port : ports) {
            if (sb.length() > 0) {
                sb.append(",");
            }
            sb.append("localhost:").append(port);
        }
        return sb.toString();
    }

    public void startup() {
        for (int i = 0; i < ports.size(); i++) {
            Integer port = ports.get(i);
            File logDir = TestUtils.constructTempDir("kafka-local");

            Map<String, String> properties = new HashMap<String, String>();
            properties.putAll(baseProperties);
            properties.put("zookeeper.connect", zkConnection);
            properties.put("broker.id", String.valueOf(i + 1));
            properties.put("host.name", "localhost");
            properties.put("port", Integer.toString(port));
            properties.put("log.dir", logDir.getAbsolutePath());
            properties.put("log.flush.interval.messages", String.valueOf(1));

            KafkaServer broker = startBroker(properties);

            brokers.add(broker);
            logDirs.add(logDir);
        }
    }


    private KafkaServer startBroker(Map<String, String> props) {
        KafkaServer server = new KafkaServer(new KafkaConfig(props), SystemTime$.MODULE$,
                Some.apply("embedded-kafka-cluster"), Vector$.MODULE$.<KafkaMetricsReporter>empty());
        server.startup();
        return server;
    }

    public Map<String, String> getProps() {
        final Map<String,String> props = new HashMap<String, String>();
        props.putAll(baseProperties);
        props.put("metadata.broker.list", brokerList);
        props.put("zookeeper.connect", zkConnection);
        return props;
    }

    public String getBrokerList() {
        return brokerList;
    }

    public List<Integer> getPorts() {
        return ports;
    }

    public String getZkConnection() {
        return zkConnection;
    }

    public void shutdown() {
        for (KafkaServer broker : brokers) {
            try {
                broker.shutdown();
                broker.awaitShutdown();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        for (File logDir : logDirs) {
            try {
                TestUtils.deleteFile(logDir);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
        }
    }

    public void awaitShutdown() {
        for (KafkaServer broker : brokers) {
                broker.awaitShutdown();
        }
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("EmbeddedKafkaCluster{");
        sb.append("boostrapServers='").append(brokerList).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
