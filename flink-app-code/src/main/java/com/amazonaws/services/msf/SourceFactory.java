package com.amazonaws.services.msf;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

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
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long RETRY_DELAY_MS = 1000;
    private static final int CONNECTION_TIMEOUT_MS = 5000;
    
    private static void validateKafkaConfig(String topic, String username, String password, String brokerUrl) {
        if (topic == null || topic.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic cannot be null or empty");
        }
        if (username == null || username.trim().isEmpty()) {
            throw new IllegalArgumentException("Username cannot be null or empty");
        }
        if (password == null || password.trim().isEmpty()) {
            throw new IllegalArgumentException("Password cannot be null or empty");
        }
        if (brokerUrl == null || brokerUrl.trim().isEmpty()) {
            throw new IllegalArgumentException("Broker URL cannot be null or empty");
        }
        if (!brokerUrl.startsWith("bootstrap.servers://")) {
            throw new IllegalArgumentException("Invalid broker URL format. Must start with 'bootstrap.servers://'");
        }
    }

    private static void testKafkaConnection(Properties properties, String brokerUrl) {
        try (org.apache.kafka.clients.admin.AdminClient adminClient =
                org.apache.kafka.clients.admin.AdminClient.create(properties)) {
            adminClient.listTopics().names().get(CONNECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS);
            LOG.info("Successfully connected to Kafka broker at {}", brokerUrl);
        } catch (Exception e) {
            LOG.error("Failed to connect to Kafka broker at {}", brokerUrl, e);
            throw new RuntimeException("Failed to connect to Kafka broker", e);
        }
    }

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
        validateKafkaConfig(topic, username, password, mskBrokerUrl);
        
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
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, "500"); // Limit batch size
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false"); // Disable auto commit for better control

        // Test connection before creating source
        testKafkaConnection(properties, mskBrokerUrl);

        Exception lastException = null;
        for (int attempt = 0; attempt < MAX_RETRY_ATTEMPTS; attempt++) {
            try {
                KafkaSource<String> source = KafkaSource.<String>builder()
                        .setBootstrapServers(mskBrokerUrl)
                        .setTopics(topic)
                        .setGroupId("flink-consumer-group")
                        .setStartingOffsets(OffsetsInitializer.earliest())
                        .setValueOnlyDeserializer(new SimpleStringSchema())
                        .setProperties(properties)
                        .build();
                
                LOG.info("Successfully created Kafka source for topic: {}", topic);
                return source;
            } catch (Exception e) {
                lastException = e;
                LOG.warn("Attempt {} failed to create Kafka source for topic: {}. Error: {}",
                    attempt + 1, topic, e.getMessage());
                
                if (attempt < MAX_RETRY_ATTEMPTS - 1) {
                    try {
                        Thread.sleep(RETRY_DELAY_MS * (attempt + 1)); // Exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while retrying source creation", ie);
                    }
                }
            }
        }
        
        LOG.error("Failed to create Kafka source after {} attempts for topic: {}", MAX_RETRY_ATTEMPTS, topic);
        throw new RuntimeException("Failed to create Kafka source after " + MAX_RETRY_ATTEMPTS + " attempts",
            lastException);
    }

    /**
     * Creates a Kafka sink with the specified configuration
     */
    public static KafkaSink<String> createMSKSink(String topic, String username, String password, String mskBrokerUrl) {
        validateKafkaConfig(topic, username, password, mskBrokerUrl);

        Properties properties = new Properties();
        String saslJaasConfig = String.format(
                "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";",
                username, password);

        properties.setProperty(SaslConfigs.SASL_MECHANISM, "SCRAM-SHA-512");
        properties.setProperty(SaslConfigs.SASL_JAAS_CONFIG, saslJaasConfig);
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, mskBrokerUrl);
        properties.setProperty(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, "SASL_SSL");

        // Test connection before creating sink
        testKafkaConnection(properties, mskBrokerUrl);

        Exception lastException = null;
        for (int attempt = 0; attempt < MAX_RETRY_ATTEMPTS; attempt++) {
            try {
                KafkaSink<String> sink = KafkaSink.<String>builder()
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

                LOG.info("Successfully created Kafka sink for topic: {}", topic);
                return sink;
            } catch (Exception e) {
                lastException = e;
                LOG.warn("Attempt {} failed to create Kafka sink for topic: {}. Error: {}",
                    attempt + 1, topic, e.getMessage());
                
                if (attempt < MAX_RETRY_ATTEMPTS - 1) {
                    try {
                        Thread.sleep(RETRY_DELAY_MS * (attempt + 1)); // Exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("Interrupted while retrying sink creation", ie);
                    }
                }
            }
        }
        
        LOG.error("Failed to create Kafka sink after {} attempts for topic: {}", MAX_RETRY_ATTEMPTS, topic);
        throw new RuntimeException("Failed to create Kafka sink after " + MAX_RETRY_ATTEMPTS + " attempts",
            lastException);
    }
}
