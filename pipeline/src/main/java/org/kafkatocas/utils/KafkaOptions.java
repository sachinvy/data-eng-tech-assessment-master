package org.kafkatocas.utils;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface KafkaOptions extends PipelineOptions {
    @Description("Kafka bootstrap servers")
    @Validation.Required
    String getBootstrapServers();

    void setBootstrapServers(String value);

    @Description("Kafka topic")
    @Validation.Required
    String getTopic();

    void setTopic(String value);

    @Description("input file path")
    @Validation.Required
    String getFileFullPath();

    void setFileFullPath(String value);

    @Description("reference file path")
    @Validation.Required
    String getReferenceFilePath();

    void setReferenceFilePath(String value);

    @Description("cassandra host")
    @Validation.Required
    String getCassandraHost();

    void setCassandraHost(String value);

    @Description("cassandra port")
    @Validation.Required
    int getCassandraPort();

    void setCassandraPort(int value);
}