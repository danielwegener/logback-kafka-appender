package com.github.danielwegener.logback.kafka;

import com.github.danielwegener.logback.kafka.util.TestKafka;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.consumer.Whitelist;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ErrorCollector;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.Collections;

import static org.junit.Assert.assertThat;

public class LogbackIntegrationIT {

    @Rule
    public ErrorCollector collector= new ErrorCollector();

    private TestKafka kafka;
    private org.slf4j.Logger logger;

    @Before
    public void beforeLogSystemInit() throws IOException, InterruptedException {
        kafka = TestKafka.createTestKafka(Collections.singletonList(9092));
        logger = LoggerFactory.getLogger("LogbackIntegrationIT");

    }

    @After
    public void tearDown() {
        kafka.shutdown();
        kafka.awaitShutdown();
    }


    @Test
    public void testLogging() throws InterruptedException {

        for (int i = 0; i<1000; ++i) {
            logger.info("message"+i);
        }

        final KafkaStream<byte[], byte[]> log = kafka.createClient().createMessageStreamsByFilter(new Whitelist("logs"),1).get(0);
        final ConsumerIterator<byte[], byte[]> iterator = log.iterator();

        for (int i=0; i<1000; ++i) {
            final String messageFromKafka = new String(iterator.next().message(), UTF8);
            assertThat(messageFromKafka, Matchers.equalTo("message"+i));
        }

    }

    private static final Charset UTF8 = Charset.forName("UTF-8");

}
