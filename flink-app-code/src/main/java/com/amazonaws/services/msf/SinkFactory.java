package com.amazonaws.services.msf;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.http.HttpHost;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.opensearch.sink.OpensearchSink;
import org.apache.flink.connector.opensearch.sink.OpensearchSinkBuilder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.opensearch.client.Requests;

/**
 * Factory class for creating and managing sinks
 */
public class SinkFactory {
    private static final Logger LOG = LoggerFactory.getLogger(SinkFactory.class);

    /**
     * Initialize all sinks for the streaming job
     */
    public static void initializeSinks(
            DataStream<Tuple9<String, Double, Double, Double, Double, Double, String, Double, String>>[] streams,
            Config config) {
        try {
            // Create OpenSearch sink
            OpensearchSink<Tuple9<String, Double, Double, Double, Double, Double, String, Double, String>> openSearchSink = 
                createOpenSearchSink(
                    config.getOpenSearchEndpoint(),
                    config.getOpenSearchPort(),
                    config.getOpenSearchUsername(),
                    config.getOpenSearchPassword()
                );

            // Create Kafka sinks
            KafkaSink<String> kafkaSink1 = SourceFactory.createMSKSink(
                config.getTopic1Enhanced(),
                config.getMskUsername(),
                config.getMskPassword(),
                config.getMskBrokerUrl()
            );

            KafkaSink<String> kafkaSink2 = SourceFactory.createMSKSink(
                config.getTopic2Enhanced(),
                config.getMskUsername(),
                config.getMskPassword(),
                config.getMskBrokerUrl()
            );

            // Connect streams to sinks
            for (int i = 0; i < streams.length; i++) {
                streams[i].sinkTo(openSearchSink);
                DataStream<String> stringStream = StreamProcessor.tuple9ToString(streams[i]);
                if (i == 0) {
                    stringStream.sinkTo(kafkaSink1);
                } else {
                    stringStream.sinkTo(kafkaSink2);
                }
            }
        } catch (Exception e) {
            LOG.error("Failed to initialize sinks", e);
            throw new RuntimeException("Failed to initialize sinks", e);
        }
    }

    /**
     * Create OpenSearch sink with specified configuration
     */
    private static OpensearchSink<Tuple9<String, Double, Double, Double, Double, Double, String, Double, String>> createOpenSearchSink(
            String endpoint, Integer port, String username, String password) {
        try {
            return new OpensearchSinkBuilder<Tuple9<String, Double, Double, Double, Double, Double, String, Double, String>>()
                .setBulkFlushMaxActions(1)
                .setHosts(new HttpHost(endpoint, port, "https"))
                .setEmitter((element, context, indexer) -> {
                    try {
                        String timestamp = element.f6;
                        DateTimeFormatter inputFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                        DateTimeFormatter outputFormatter = DateTimeFormatter.ISO_DATE_TIME;
                        
                        String formattedTimestamp = LocalDateTime
                            .parse(timestamp, inputFormatter)
                            .format(outputFormatter);

                        indexer.add(
                            Requests.indexRequest()
                                .index(element.f0.toLowerCase())
                                .source(Map.ofEntries(
                                    Map.entry("close", element.f1),
                                    Map.entry("open", element.f2),
                                    Map.entry("low", element.f3),
                                    Map.entry("high", element.f4),
                                    Map.entry("volume", element.f5),
                                    Map.entry("timestamp", formattedTimestamp),
                                    Map.entry("%change", element.f7),
                                    Map.entry("indicator", element.f8))));
                    } catch (Exception e) {
                        LOG.error("Failed to process record for OpenSearch: {}", element, e);
                        throw e;
                    }
                })
                .setConnectionUsername(username)
                .setConnectionPassword(password)
                .build();
        } catch (Exception e) {
            LOG.error("Failed to create OpenSearch sink", e);
            throw new RuntimeException("Failed to create OpenSearch sink", e);
        }
    }
}
