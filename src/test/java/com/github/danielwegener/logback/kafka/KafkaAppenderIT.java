package com.github.danielwegener.logback.kafka;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.PatternLayout;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.classic.spi.LoggingEvent;
import ch.qos.logback.core.status.Status;
import ch.qos.logback.core.status.StatusListener;
import com.github.danielwegener.logback.kafka.encoding.PatternLayoutKafkaMessageEncoder;
import com.github.danielwegener.logback.kafka.keying.RoundRobinKeyingStrategy;
import com.github.danielwegener.logback.kafka.util.TestKafka;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import kafka.javaapi.consumer.ConsumerConnector;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Properties;
import java.util.Random;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


public class KafkaAppenderIT {

    @Rule
    public ErrorCollector collector= new ErrorCollector();

    private TestKafka kafka;
    private KafkaAppenderBase<ILoggingEvent> unit;

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

        unit = new KafkaAppenderBase<ILoggingEvent>();
        final PatternLayout patternLayout = new PatternLayout();
        patternLayout.setPattern("%msg");
        patternLayout.setContext(loggerContext);
        patternLayout.start();
        unit.setEncoder(new PatternLayoutKafkaMessageEncoder(patternLayout, Charset.forName("UTF-8")));
        unit.setTopic("logs");
        unit.setName("TestKafkaAppender");
        unit.setContext(loggerContext);
        unit.addProducerConfigValue(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBrokerList());
        unit.setKeyingStrategy(new RoundRobinKeyingStrategy());
    }

    @After
    public void tearDown() {
        kafka.shutdown();
        kafka.awaitShutdown();
    }


    @Test
    public void testLogging() throws InterruptedException {

        final Logger logger = loggerContext.getLogger("ROOT");

        unit.start();

        assertTrue("appender is started", unit.isStarted());

        for (int i = 0; i<1000; ++i) {
            final LoggingEvent loggingEvent = new LoggingEvent("a.b.c.d", logger, Level.INFO, "message"+i, null, new Object[0]);
            unit.append(loggingEvent);
        }

        final Properties consumerProperties = new Properties();
        consumerProperties.put("metadata.broker.list", kafka.getBrokerList());
        consumerProperties.put("group.id", "simple-consumer-" + new Random().nextInt());
        consumerProperties.put("auto.commit.enable","false");
        consumerProperties.put("auto.offset.reset","smallest");
        consumerProperties.put("zookeeper.connect", kafka.getZookeeperConnection());
        final kafka.consumer.ConsumerConfig consumerConfig = new kafka.consumer.ConsumerConfig(consumerProperties);
        final ConsumerConnector javaConsumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
        final KafkaStream<byte[], byte[]> log = javaConsumerConnector.createMessageStreamsByFilter(new Whitelist("logs"),1).get(0);
        final ConsumerIterator<byte[], byte[]> iterator = log.iterator();

        for (int i=0; i<1000; ++i) {
            final String messageFromKafka = new String(iterator.next().message(), UTF8);
            assertThat(messageFromKafka, Matchers.equalTo("message"+i));
        }

    }

    private static final Charset UTF8 = Charset.forName("UTF-8");

}