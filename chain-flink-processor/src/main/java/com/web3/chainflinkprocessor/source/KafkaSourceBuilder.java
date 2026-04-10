package com.web3.chainflinkprocessor.source;

import com.web3.chainflinkprocessor.config.AppConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;

public class KafkaSourceBuilder {
    public static KafkaSource<String> build(String topic) {
        return KafkaSource.<String>builder()
                .setBootstrapServers(AppConfig.KAFKA_BROKERS)
                .setTopics(topic)
                .setGroupId(AppConfig.KAFKA_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }
}