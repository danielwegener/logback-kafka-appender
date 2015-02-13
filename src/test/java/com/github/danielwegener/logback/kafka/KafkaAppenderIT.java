package com.github.danielwegener.logback.kafka;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.status.Status;
import ch.qos.logback.core.status.StatusListener;
import com.github.danielwegener.logback.kafka.encoding.PatternLayoutKafkaEncoder;
import com.github.danielwegener.logback.kafka.partitioning.RoundRobinPartitioningStrategy;
import com.github.danielwegener.logback.kafka.util.TestKafka;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.TopicFilter;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import scala.Function1;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;


public class KafkaAppenderIT {

    @Rule
    public ErrorCollector collector= new ErrorCollector();

    private TestKafka kafka;
    private KafkaAppender<ILoggingEvent> unit;

    private LoggerContext loggerContext;

    @Before
    public void beforeLogSystemInit() throws IOException, InterruptedException {

        kafka = TestKafka.createTestKafka(1);

        loggerContext = new LoggerContext();
        loggerContext.putProperty("brokers.list", kafka.getBrokerList());
        loggerContext.getStatusManager().add(new StatusListener() {
            @Override
            public void addStatusEvent(Status status) {
                if (status.getEffectiveLevel() > Status.INFO) {
                    System.err.println(status.toString());
                    if (status.getThrowable() != null) {
                        collector.addError(status.getThrowable());
                    } else {
                        collector.addError(new RuntimeException("StatusManager reported warning: "+status.toString()));
                    }
                } else {
                    System.out.println(status.toString());
                }
            }
        });
        loggerContext.putProperty("HOSTNAME","localhost");

        unit = new KafkaAppender<ILoggingEvent>();
        final PatternLayout patternLayout = new PatternLayout();
        patternLayout.setPattern("%m");
        unit.setEncoder(new PatternLayoutKafkaEncoder(patternLayout, Charset.forName("UTF-8")));
        unit.setTopic("logs");
        unit.setName("TestKafkaAppender");
        unit.setContext(loggerContext);
        unit.addProducerConfigValue(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBrokerList());
        unit.setPartitioningStrategy(new RoundRobinPartitioningStrategy<ILoggingEvent>());
    }

    @After
    public void tearDown() {
        kafka.shutdown();
        kafka.awaitShutdown();
    }

    @Test
    public void noop() {

    }

    @Test
    public void testLogging() throws InterruptedException {
        //org.slf4j.Logger exampleLogger = org.slf4j.LoggerFactory.getLogger("IT");

        final Logger logger = loggerContext.getLogger("ROOT");
        final LoggingEvent loggingEvent = new LoggingEvent("a.b.c.d", logger, Level.INFO, "message", null, new Object[0]);
        unit.start();

        System.err.println("in test");

        Assert.assertTrue("appender is started", unit.isStarted());
        Thread.sleep(5000);
        unit.append(loggingEvent);

        final Properties consumerProperties = new Properties();
        consumerProperties.put("metadata.broker.list", kafka.getBrokerList());
        consumerProperties.put("group.id", "simple-consumer-"+ ThreadLocalRandom.current().nextInt());
        consumerProperties.put("auto.commit.enable","false");
        consumerProperties.put("auto.offset.reset","smallest");
        consumerProperties.put("zookeeper.connect", kafka.getZookeeperConnection());
        final kafka.consumer.ConsumerConfig consumerConfig = new kafka.consumer.ConsumerConfig(consumerProperties);
        final ConsumerConnector javaConsumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        final KafkaStream<byte[], byte[]> log = javaConsumerConnector.createMessageStreamsByFilter(new Whitelist("logs"),1).get(0);
        ConsumerIterator<byte[], byte[]> iterator = log.iterator();
        while (iterator.hasNext()) {
            System.err.println(new String(iterator.next().message(), UTF8));
        }


    }

    private static final Charset UTF8 = Charset.forName("UTF-8");

}