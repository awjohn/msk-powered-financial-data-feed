package com.amazonaws.services.msf;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingJob {
    private static final Logger LOG = LoggerFactory.getLogger(StreamingJob.class);
    private static final Config config = new Config();
    private static final int DEFAULT_PARALLELISM = 4;
    private static final int DEFAULT_CHECKPOINT_INTERVAL = 60000; // 1 minute
    private static final int DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS = 30000; // 30 seconds
    private static final int DEFAULT_CHECKPOINT_TIMEOUT = 900000; // 15 minutes

    /**
     * Main entry point for the Flink streaming job.
     * Initializes environment, sources, transformations and sinks.
     */
    public static void main(String[] args) {
        StreamExecutionEnvironment env = null;
        try {
            env = setupEnvironment();
            final ParameterTool applicationProperties = ConfigLoader.loadApplicationParameters(args, env);
            LOG.info("Application properties loaded: {}", applicationProperties.toMap());

            // Initialize configuration
            validateAndInitializeConfig(applicationProperties);
            
            // Create and configure sources
            DataStream<String>[] inputStreams = createAndValidateSources(env);
            
            // Map streams to tuples and perform transformations
            DataStream<Tuple9<String, Double, Double, Double, Double, Double, String, Double, String>>[] outputStreams =
                StreamProcessor.processStreams(inputStreams, config.getEventTickerInterval());
            
            // Configure and initialize sinks
            SinkFactory.initializeSinks(outputStreams, config);
            
            // Execute the job
            env.execute("Flink stock streaming app");
        } catch (Exception e) {
            LOG.error("Failed to execute streaming job", e);
            System.exit(1);
        }
    }

    private static StreamExecutionEnvironment setupEnvironment() {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        
        // Configure checkpointing
        env.enableCheckpointing(DEFAULT_CHECKPOINT_INTERVAL);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS);
        env.getCheckpointConfig().setCheckpointTimeout(DEFAULT_CHECKPOINT_TIMEOUT);
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(3);
        
        // Configure parallelism
        env.setParallelism(DEFAULT_PARALLELISM);
        
        // Configure restart strategy
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
            3, // number of restart attempts
            Time.seconds(10) // delay between attempts
        ));

        return env;
    }

    private static void validateAndInitializeConfig(ParameterTool applicationProperties) {
        try {
            config.initialize(applicationProperties);
        } catch (Exception e) {
            LOG.error("Failed to initialize configuration", e);
            throw new RuntimeException("Configuration initialization failed", e);
        }

        // Validate essential configuration
        if (config.getMskBrokerUrl() == null || config.getMskBrokerUrl().trim().isEmpty()) {
            throw new IllegalArgumentException("MSK broker URL is not configured");
        }
        if (config.getEventTicker1() == null || config.getEventTicker2() == null) {
            throw new IllegalArgumentException("Event tickers are not properly configured");
        }
    }

    private static DataStream<String>[] createAndValidateSources(StreamExecutionEnvironment env) {
        DataStream<String>[] inputStreams = SourceFactory.createKafkaSources(env, config);
        if (inputStreams == null || inputStreams.length != 2) {
            throw new RuntimeException("Failed to create required Kafka sources");
        }
        return inputStreams;
    }
}
