package com.amazonaws.services.msf;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 * Configuration class to handle all application parameters and defaults
 */
public class Config {
    // Constants for configuration defaults
    public static final long DEFAULT_TICKER_INTERVAL = 1; // In minutes
    public static final Integer DEFAULT_OPENSEARCH_PORT = 443;
    public static final String DEFAULT_OPENSEARCH_USERNAME = "admin";
    public static final String DEFAULT_OPENSEARCH_PASSWORD = "Test@123";
    public static final String DEFAULT_AWS_REGION = "eu-east-1";

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
        mskUsername = requireNonNull(properties.get("msk.username"), "MSK username is required");
        mskPassword = requireNonNull(properties.get("msk.password"), "MSK password is required");
        mskBrokerUrl = requireNonNull(properties.get("msk.broker.url"), "MSK broker URL is required");
        
        openSearchEndpoint = requireNonNull(properties.get("opensearch.endpoint"), "OpenSearch endpoint is required");
        openSearchPort = Integer.valueOf(properties.get("opensearch.port", String.valueOf(DEFAULT_OPENSEARCH_PORT)));
        openSearchUsername = properties.get("opensearch.username", DEFAULT_OPENSEARCH_USERNAME);
        openSearchPassword = properties.get("opensearch.password", DEFAULT_OPENSEARCH_PASSWORD);
        
        eventTicker1 = requireNonNull(properties.get("event.ticker.1"), "Event ticker 1 is required");
        eventTicker2 = requireNonNull(properties.get("event.ticker.2"), "Event ticker 2 is required");
        topic1enhanced = requireNonNull(properties.get("topic.ticker.1"), "Topic ticker 1 is required");
        topic2enhanced = requireNonNull(properties.get("topic.ticker.2"), "Topic ticker 2 is required");
        
        String intervalStr = properties.get("event.ticker.interval.minutes");
        eventTickerInterval = (intervalStr != null ? Long.parseLong(intervalStr) : DEFAULT_TICKER_INTERVAL) * 60 * 1000;
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
