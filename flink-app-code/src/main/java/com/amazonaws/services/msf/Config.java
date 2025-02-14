package com.amazonaws.services.msf;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Configuration class to handle all application parameters and defaults
 */
public class Config {
    // Constants for configuration defaults
    public static final long DEFAULT_TICKER_INTERVAL = 1; // In minutes
    public static final Integer DEFAULT_OPENSEARCH_PORT = 443;
    public static final String DEFAULT_AWS_REGION = "us-east-1";
    
    // Remove hardcoded credentials - require explicit configuration
    public static final String DEFAULT_OPENSEARCH_USERNAME = "";
    public static final String DEFAULT_OPENSEARCH_PASSWORD = "";

    private String mskUsername;
    private String mskPassword;
    private String mskBrokerUrl;
    private String openSearchEndpoint;
    private Integer openSearchPort;
    private String openSearchUsername;
    private String openSearchPassword;
    private String eventTicker1;
    private String eventTicker2;
    private String topic1enhanced;
    private String topic2enhanced;
    private long eventTickerInterval;

    public void initialize(ParameterTool properties) {
        validateAndSetProperties(properties);
    }

    private void validateAndSetProperties(ParameterTool properties) {
        // Validate and set MSK properties
        validateMskProperties(properties);
        
        // Validate and set OpenSearch properties
        validateOpenSearchProperties(properties);
        
        // Validate and set event ticker properties
        validateEventTickerProperties(properties);
        
        // Set event ticker interval with validation
        setEventTickerInterval(properties);
    }

    private void validateMskProperties(ParameterTool properties) {
        mskUsername = requireNonNull(properties.get("msk.username"), "MSK username is required");
        mskPassword = requireNonNull(properties.get("msk.password"), "MSK password is required");
        mskBrokerUrl = requireNonNull(properties.get("msk.broker.url"), "MSK broker URL is required");
        
        if (!mskBrokerUrl.startsWith("https://") && !mskBrokerUrl.startsWith("http://")) {
            throw new IllegalArgumentException("MSK broker URL must start with http:// or https://");
        }
    }

    private void validateOpenSearchProperties(ParameterTool properties) {
        openSearchEndpoint = requireNonNull(properties.get("opensearch.endpoint"), "OpenSearch endpoint is required");
        
        // Validate port number
        String portStr = properties.get("opensearch.port", String.valueOf(DEFAULT_OPENSEARCH_PORT));
        try {
            openSearchPort = Integer.valueOf(portStr);
            if (openSearchPort <= 0 || openSearchPort > 65535) {
                throw new IllegalArgumentException("OpenSearch port must be between 1 and 65535");
            }
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid OpenSearch port number: " + portStr);
        }

        // Require explicit OpenSearch credentials
        openSearchUsername = requireNonNull(properties.get("opensearch.username"), "OpenSearch username is required");
        openSearchPassword = requireNonNull(properties.get("opensearch.password"), "OpenSearch password is required");
        
        if (!openSearchEndpoint.startsWith("https://") && !openSearchEndpoint.startsWith("http://")) {
            throw new IllegalArgumentException("OpenSearch endpoint must start with http:// or https://");
        }
    }

    private void validateEventTickerProperties(ParameterTool properties) {
        eventTicker1 = requireNonNull(properties.get("event.ticker.1"), "Event ticker 1 is required");
        eventTicker2 = requireNonNull(properties.get("event.ticker.2"), "Event ticker 2 is required");
        topic1enhanced = requireNonNull(properties.get("topic.ticker.1"), "Topic ticker 1 is required");
        topic2enhanced = requireNonNull(properties.get("topic.ticker.2"), "Topic ticker 2 is required");
        
        // Validate ticker symbols format
        validateTickerFormat(eventTicker1, "event.ticker.1");
        validateTickerFormat(eventTicker2, "event.ticker.2");
    }

    private void validateTickerFormat(String ticker, String propertyName) {
        if (!ticker.matches("^[A-Z0-9.]+$")) {
            throw new IllegalArgumentException(
                String.format("Invalid ticker format for %s: %s. Must contain only uppercase letters, numbers, and dots.",
                    propertyName, ticker));
        }
    }

    private void setEventTickerInterval(ParameterTool properties) {
        String intervalStr = properties.get("event.ticker.interval.minutes");
        try {
            long interval = intervalStr != null ? Long.parseLong(intervalStr) : DEFAULT_TICKER_INTERVAL;
            if (interval <= 0) {
                throw new IllegalArgumentException("Event ticker interval must be positive");
            }
            eventTickerInterval = interval * 60 * 1000; // Convert to milliseconds
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid event ticker interval: " + intervalStr);
        }
    }

    private String requireNonNull(String value, String message) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(message);
        }
        return value;
    }

    // Getters
    public String getMskUsername() { return mskUsername; }
    public String getMskPassword() { return mskPassword; }
    public String getMskBrokerUrl() { return mskBrokerUrl; }
    public String getOpenSearchEndpoint() { return openSearchEndpoint; }
    public Integer getOpenSearchPort() { return openSearchPort; }
    public String getOpenSearchUsername() { return openSearchUsername; }
    public String getOpenSearchPassword() { return openSearchPassword; }
    public String getEventTicker1() { return eventTicker1; }
    public String getEventTicker2() { return eventTicker2; }
    public String getTopic1Enhanced() { return topic1enhanced; }
    public String getTopic2Enhanced() { return topic2enhanced; }
    public long getEventTickerInterval() { return eventTickerInterval; }
}
