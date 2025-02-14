package com.amazonaws.services.msf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingJob {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);
    private static final Config config = new Config();

    /**
     * Main entry point for the Flink streaming job.
     * Initializes environment, sources, transformations and sinks.
     */
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        final ParameterTool applicationProperties = ConfigLoader.loadApplicationParameters(args, env);
        LOG.info("Application properties loaded: {}", applicationProperties.toMap());

        try {
            // Initialize configuration
            config.initialize(applicationProperties);
            
            // Create and configure sources
            DataStream<String>[] inputStreams = SourceFactory.createKafkaSources(env, config);
            
            // Map streams to tuples and perform transformations
            DataStream<Tuple9<String, Double, Double, Double, Double, Double, String, Double, String>>[] outputStreams = 
                StreamProcessor.processStreams(inputStreams, config.getEventTickerInterval());
            
            // Configure and initialize sinks
            SinkFactory.initializeSinks(outputStreams, config);
            
            env.execute("Flink stock streaming app");
        } catch (Exception e) {
            LOG.error("Failed to execute streaming job", e);
            throw e;
        }
    }
}
