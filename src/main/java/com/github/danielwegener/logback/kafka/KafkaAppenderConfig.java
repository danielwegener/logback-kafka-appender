package com.github.danielwegener.logback.kafka;

import ch.qos.logback.core.Layout;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import ch.qos.logback.core.status.ErrorStatus;

import java.nio.charset.Charset;

/**
 * @author Daniel Wegener (Holisticon AG)
 */
public abstract class KafkaAppenderConfig<T> extends UnsynchronizedAppenderBase<T> {

    protected String topic = null;
    protected PartitioningStrategy partitioningStrategy = null;
    protected Layout<T> layout = null;
    protected Charset charset = null;

    protected String brokerList = null;

    protected boolean checkPrerequisites() {
        boolean errorFree = true;

        if (this.brokerList == null) {
            addStatus(new ErrorStatus("No brokerList set for the appender named \""
                    + name + "\".", this));
            errorFree = false;
        }


        if (this.topic == null) {
            addStatus(new ErrorStatus("No topic set for the appender named \""
                    + name + "\".", this));
            errorFree = false;
        }

        if (this.layout == null) {
            addStatus(new ErrorStatus("No layout set for the appender named \""
                    + name + "\".", this));
            errorFree = false;
        }

        // only error free appenders should be activated
        return !errorFree;
    }


    public void setCharset(String charset) {
        this.charset = Charset.forName(charset);
    }

    public void setLayout(Layout<T> layout) {
        this.layout = layout;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public void setPartitioningStrategy(String partitioningStrategy) {
        this.partitioningStrategy = PartitioningStrategy.valueOf(partitioningStrategy);
    }

    public String getBrokerList() {
        return brokerList;
    }

    public void setBrokerList(String brokerList) {
        this.brokerList = brokerList;
    }


}
