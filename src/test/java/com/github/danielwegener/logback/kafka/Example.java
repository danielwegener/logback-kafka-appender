package com.github.danielwegener.logback.kafka;


import ch.qos.logback.classic.Logger;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

/**
 * @author Daniel Wegener (Holisticon AG)
 */
public class Example {

    @Ignore
    @Test()
    public void doIt() {
        final Logger it = (Logger) LoggerFactory.getLogger("IT");

        for (int i = 0; i<10; ++i) {
            it.info("foo"+i);
        }
        it.getLoggerContext().reset();
    }

}
