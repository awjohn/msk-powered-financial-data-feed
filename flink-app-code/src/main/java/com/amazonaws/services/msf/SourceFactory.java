package com.amazonaws.services.msf;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.config.SaslConfigs;

/**
 * Factory class for creating Kafka sources and sinks
 */
public class SourceFactory {
    private static final Logger LOG = LoggerFactory.getLogger(SourceFactory.class);

    /**
     * Creates Kafka sources for both input streams
     */
    public static DataStream<String>[] createKafkaSources(StreamExecutionEnvironment env, Config config) {
        try {
            KafkaSource<String> source1 = createKafkaSource(
                    config.getEventTicker1(),
                    config.getMskUsername(),
                    config.getMskPassword(),
                    config.getMskBrokerUrl(),
                    config.getEventTickerInterval()
            );

            KafkaSource<String> source2 = createKafkaSource(
                    config.getEventTicker2(),
                    config.getMskUsername(),
                    config.getMskPassword(),
                    config.getMskBrokerUrl(),
                    config.getEventTickerInterval()
            );

            @SuppressWarnings("unchecked")
            DataStream<String>[] streams = new DataStream[] {
                    env.fromSource(source1, WatermarkStrategy.noWatermarks(), "Kafka Source 1"),
                    env.fromSource(source2, WatermarkStrategy.noWatermarks(), "Kafka Source 2")
            };

            return streams;
        } catch (Exception e) {
            LOG.error("Failed to create Kafka sources", e);
            throw new RuntimeException("Failed to create Kafka sources", e);
        }
    }

    /**
     * Creates a Kafka source with the specified configuration
     */
    private static KafkaSource<String> createKafkaSource(String topic, String username, String password,
            String mskBrokerUrl, Long requestTimeout) {
        try {
            Properties properties = new Properties();
            String saslJaasConfig = String.format(
                    "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                    username, password);

            properties.setProperty(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
            properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer-group");
            properties.setProperty(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, requestTimeout.toString());
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringDeserializer");
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    "org.apache.kafka.common.serialization.StringDeserializer");
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, mskBrokerUrl);
            properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");

            return KafkaSource.<String>builder()
                    .setBootstrapServers(mskBrokerUrl)
                    .setTopics(topic)
                    .setGroupId("flink-consumer-group")
                    .setStartingOffsets(OffsetsInitializer.earliest())
                    .setValueOnlyDeserializer(new SimpleStringSchema())
                    .setProperties(properties)
                    .build();
        } catch (Exception e) {
            LOG.error("Failed to create Kafka source for topic: {}", topic, e);
            throw new RuntimeException("Failed to create Kafka source", e);
        }
    }

    /**
     * Creates a Kafka sink with the specified configuration
     */
    public static KafkaSink<String> createMSKSink(String topic, String username, String password, String mskBrokerUrl) {
        try {
            String saslJaasConfig = String.format(
                    "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                    username, password);

            return KafkaSink.<String>builder()
                    .setProperty(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512")
                    .setProperty(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig)
                    .setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, mskBrokerUrl)
                    .setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL")
                    .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                    .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                            .setTopic(topic)
                            .setValueSerializationSchema(new SimpleStringSchema())
                            .build())
                    .build();
        } catch (Exception e) {
            LOG.error("Failed to create Kafka sink for topic: {}", topic, e);
            throw new RuntimeException("Failed to create Kafka sink", e);
        }
    }
}
