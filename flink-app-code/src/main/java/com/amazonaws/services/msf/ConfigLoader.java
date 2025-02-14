package com.amazonaws.services.msf;

import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

/**
 * Utility class for loading application configuration parameters
 */
public class ConfigLoader {
    private static final Logger LOG = LoggerFactory.getLogger(ConfigLoader.class);
    private static final String DEFAULT_CONFIG_PATH = "application.properties";

    /**
     * Loads application parameters from command line arguments and configuration file
     */
    public static ParameterTool loadApplicationParameters(String[] args, StreamExecutionEnvironment env) {
        try {
            // Load parameters from command line
            ParameterTool commandLineParams = ParameterTool.fromArgs(args);
            
            // Get config file path from command line or use default
            String configPath = commandLineParams.get("config.path", DEFAULT_CONFIG_PATH);
            
            // Load parameters from config file if it exists
            ParameterTool fileParams = loadFromFile(configPath);
            
            // Merge parameters (command line parameters take precedence)
            java.util.Map<String, String> mergedProps = new java.util.HashMap<>();
            if (fileParams != null) {
                mergedProps.putAll(fileParams.toMap());
            }
            mergedProps.putAll(commandLineParams.toMap());
            
            // Create final ParameterTool
            ParameterTool finalParams = ParameterTool.fromMap(mergedProps);
            
            // Register parameters globally
            env.getConfig().setGlobalJobParameters(finalParams);
            
            return finalParams;
        } catch (Exception e) {
            LOG.error("Failed to load application parameters", e);
            throw new RuntimeException("Failed to load application parameters", e);
        }
    }

    /**
     * Loads parameters from a properties file
     */
    private static ParameterTool loadFromFile(String configPath) {
        try {
            if (Files.exists(Paths.get(configPath))) {
                LOG.info("Loading configuration from file: {}", configPath);
                return ParameterTool.fromPropertiesFile(configPath);
            } else {
                LOG.warn("Configuration file not found at: {}. Using defaults.", configPath);
                return null;
            }
        } catch (IOException e) {
            LOG.error("Failed to load configuration file: {}", configPath, e);
            return null;
        }
    }

    /**
     * Validates that all required parameters are present
     */
    public static void validateRequiredParameters(ParameterTool params, String... requiredParams) {
        for (String param : requiredParams) {
            if (!params.has(param)) {
                throw new IllegalArgumentException("Missing required parameter: " + param);
            }
            if (params.get(param).trim().isEmpty()) {
                throw new IllegalArgumentException("Empty value for required parameter: " + param);
            }
        }
    }
}