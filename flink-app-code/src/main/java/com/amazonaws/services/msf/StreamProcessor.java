package com.amazonaws.services.msf;

import java.time.LocalDate;
import java.time.Period;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.metrics.Counter;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * Processor class for handling stream transformations
 */
public class StreamProcessor {
    private static final Logger LOG = LoggerFactory.getLogger(StreamProcessor.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long RETRY_DELAY_MS = 1000;

    /**
     * Process input streams and apply transformations
     */
    public static DataStream<Tuple9<String, Double, Double, Double, Double, Double, String, Double, String>>[] processStreams(
            DataStream<String>[] inputStreams, long eventGap) {
        try {
            @SuppressWarnings("unchecked")
            DataStream<Tuple9<String, Double, Double, Double, Double, Double, String, Double, String>>[] outputStreams = 
                new DataStream[2];

            // Map each input stream to tuple and perform transformations
            for (int i = 0; i < inputStreams.length; i++) {
                DataStream<Tuple7<String, Double, Double, Double, Double, Double, String>> tupleStream = 
                    mapToTupleStream(inputStreams[i]);
                outputStreams[i] = performTransformations(tupleStream, eventGap);
            }

            return outputStreams;
        } catch (Exception e) {
            LOG.error("Failed to process streams", e);
            throw new RuntimeException("Failed to process streams", e);
        }
    }

    private static class StockDataMapper extends RichMapFunction<String, Tuple7<String, Double, Double, Double, Double, Double, String>> {
        private transient Counter parseErrorCounter;
        private transient Counter retryCounter;

        @Override
        public void open(Configuration parameters) {
            parseErrorCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("parse_errors");
            retryCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("retry_attempts");
        }

        @Override
        public Tuple7<String, Double, Double, Double, Double, Double, String> map(String value) throws Exception {
            Exception lastException = null;
            
            for (int attempt = 0; attempt < MAX_RETRY_ATTEMPTS; attempt++) {
                try {
                    return parseStockData(value);
                } catch (Exception e) {
                    lastException = e;
                    retryCounter.inc();
                    LOG.warn("Attempt {} failed to parse record: {}. Error: {}", 
                        attempt + 1, value, e.getMessage());
                    
                    if (attempt < MAX_RETRY_ATTEMPTS - 1) {
                        Thread.sleep(RETRY_DELAY_MS * (attempt + 1)); // Exponential backoff
                    }
                }
            }
            
            parseErrorCounter.inc();
            LOG.error("Failed to parse record after {} attempts: {}", MAX_RETRY_ATTEMPTS, value);
            throw new RuntimeException("Failed to parse record after " + MAX_RETRY_ATTEMPTS + " attempts", 
                lastException);
        }

        private Tuple7<String, Double, Double, Double, Double, Double, String> parseStockData(String value) 
                throws Exception {
            if (value == null || value.trim().isEmpty()) {
                throw new IllegalArgumentException("Empty or null record received");
            }

            JsonNode node = OBJECT_MAPPER.readTree(value);
            validateJsonFields(node);

            String stockSymbol = node.get("symbol").asText();
            Double closePrice = node.get("close").asDouble();
            Double openPrice = node.get("open").asDouble();
            Double lowPrice = node.get("low").asDouble();
            Double highPrice = node.get("high").asDouble();
            Double volume = node.get("volume").asDouble();
            String eventTime = node.get("timestamp").asText();

            validatePrices(openPrice, closePrice, highPrice, lowPrice, stockSymbol);
            validateVolume(volume, stockSymbol);

            LOG.debug("Successfully mapped record: [{}, {}, {}, {}, {}, {}, {}]",
                stockSymbol, closePrice, openPrice, lowPrice, highPrice, volume, eventTime);

            return new Tuple7<>(stockSymbol, closePrice, openPrice, lowPrice,
                highPrice, volume, eventTime);
        }

        private void validateJsonFields(JsonNode node) {
            String[] requiredFields = {"symbol", "close", "open", "low", "high", "volume", "timestamp"};
            for (String field : requiredFields) {
                if (!node.has(field)) {
                    throw new IllegalArgumentException("Missing required field: " + field);
                }
            }
        }

        private void validatePrices(Double open, Double close, Double high, Double low, String symbol) {
            if (open < 0 || close < 0 || high < 0 || low < 0) {
                throw new IllegalArgumentException(
                    String.format("Invalid negative price values for symbol %s", symbol));
            }
            if (low > high) {
                throw new IllegalArgumentException(
                    String.format("Low price (%f) is greater than high price (%f) for symbol %s", 
                        low, high, symbol));
            }
        }

        private void validateVolume(Double volume, String symbol) {
            if (volume < 0) {
                throw new IllegalArgumentException(
                    String.format("Invalid negative volume value for symbol %s", symbol));
            }
        }
    }

    private static DataStream<Tuple7<String, Double, Double, Double, Double, Double, String>> mapToTupleStream(
            DataStream<String> inputStream) {
        return inputStream
            .map(new StockDataMapper())
            .name("Stock Data Parser")
            .setParallelism(4) // Adjust based on your needs
            .returns(TypeInformation.of(new TypeHint<Tuple7<String, Double, Double, Double, Double, Double, String>>() {}));
    }

    private static class StockPriceProcessor extends ProcessWindowFunction<
            Tuple7<String, Double, Double, Double, Double, Double, String>,
            Tuple9<String, Double, Double, Double, Double, Double, String, Double, String>,
            String,
            TimeWindow> {
        private ValueState<Tuple7<String, Double, Double, Double, Double, Double, String>> lastDayObjectState;
        private ValueState<Tuple7<String, Double, Double, Double, Double, Double, String>> lastProcessedObjectState;
        private transient Counter processedRecordsCounter;
        private transient Counter stateUpdatesCounter;
        private transient Counter processingErrorsCounter;

        @Override
        public void open(Configuration parameters) throws Exception {
            // Initialize state descriptors with TTL to prevent memory leaks
            ValueStateDescriptor<Tuple7<String, Double, Double, Double, Double, Double, String>> lastProcessedDescriptor = 
                new ValueStateDescriptor<>(
                    "lastProcessedObjectState",
                    TypeInformation.of(
                        new TypeHint<Tuple7<String, Double, Double, Double, Double, Double, String>>() {}
                    )
                );
            lastProcessedDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(7))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build());
            lastProcessedObjectState = getRuntimeContext().getState(lastProcessedDescriptor);

            ValueStateDescriptor<Tuple7<String, Double, Double, Double, Double, Double, String>> lastDayDescriptor =
                new ValueStateDescriptor<>(
                    "lastDayObjectState",
                    TypeInformation.of(
                        new TypeHint<Tuple7<String, Double, Double, Double, Double, Double, String>>() {}
                    )
                );
            lastDayDescriptor.enableTimeToLive(StateTtlConfig.newBuilder(org.apache.flink.api.common.time.Time.days(7))
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build());
            lastDayObjectState = getRuntimeContext().getState(lastDayDescriptor);

            // Initialize metrics
            processedRecordsCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("processed_records_total");
            stateUpdatesCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("state_updates_total");
            processingErrorsCounter = getRuntimeContext()
                .getMetricGroup()
                .counter("processing_errors_total");
        }

        @Override
        public void process(
                String key,
                Context context,
                Iterable<Tuple7<String, Double, Double, Double, Double, Double, String>> records,
                Collector<Tuple9<String, Double, Double, Double, Double, Double, String, Double, String>> out)
                throws Exception {
            LOG.info("Processing Session window for key: {} at time: {}", key, context.window().getEnd());

            try {
                Tuple7<String, Double, Double, Double, Double, Double, String> lastDayObject = lastDayObjectState.value();
                Tuple7<String, Double, Double, Double, Double, Double, String> lastProcessedObject = lastProcessedObjectState.value();

                for (Tuple7<String, Double, Double, Double, Double, Double, String> record : records) {
                    try {
                        processRecord(record, lastDayObject, lastProcessedObject, out);
                        updateState(record, lastProcessedObject);
                        processedRecordsCounter.inc();
                    } catch (Exception e) {
                        processingErrorsCounter.inc();
                        LOG.error("Failed to process record for key: {}, Error: {}", key, e.getMessage());
                        // Continue processing other records
                    }
                }
            } catch (Exception e) {
                processingErrorsCounter.inc();
                LOG.error("Critical error processing window for key: {}, Error: {}", key, e.getMessage());
                throw e; // Rethrow critical errors
            }
        }

        private void processRecord(
                Tuple7<String, Double, Double, Double, Double, Double, String> record,
                Tuple7<String, Double, Double, Double, Double, Double, String> lastDayObject,
                Tuple7<String, Double, Double, Double, Double, Double, String> lastProcessedObject,
                Collector<Tuple9<String, Double, Double, Double, Double, Double, String, Double, String>> out) {
            try {
                if (lastDayObject != null) {
                    Double diff = record.f1 - lastDayObject.f1;
                    Double percentageChange = (diff / lastDayObject.f1) * 100;
                    String indicator = calculateIndicator(percentageChange);

                    out.collect(Tuple9.of(
                            record.f0, record.f1, record.f2, record.f3, record.f4,
                            record.f5, record.f6, percentageChange, indicator));
                } else {
                    out.collect(Tuple9.of(
                            record.f0, record.f1, record.f2, record.f3, record.f4,
                            record.f5, record.f6, 0.00, "Neutral"));
                }
            } catch (Exception e) {
                LOG.error("Failed to process record: {}", record, e);
                throw new RuntimeException("Failed to process record", e);
            }
        }

        private void updateState(
                Tuple7<String, Double, Double, Double, Double, Double, String> record,
                Tuple7<String, Double, Double, Double, Double, Double, String> lastProcessedObject) 
                throws Exception {
            try {
                if (lastProcessedObject != null) {
                    Integer dayDifference = calculateDayDifference(lastProcessedObject.f6, record.f6);
                    if (dayDifference >= 1) {
                        LOG.info("Updating state for new date");
                        lastDayObjectState.update(lastProcessedObject);
                        stateUpdatesCounter.inc();
                    }
                }
                lastProcessedObjectState.update(record);
                stateUpdatesCounter.inc();
            } catch (Exception e) {
                LOG.error("Failed to update state for record: {}", record, e);
                throw e;
            }
        }

        private String calculateIndicator(Double percentageChange) {
            if (percentageChange > 5) return "Positive";
            if (percentageChange < -5) return "Negative";
            return "Neutral";
        }

        private Integer calculateDayDifference(String oldDate, String newDate) {
            try {
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                LocalDate lastDate = LocalDate.parse(oldDate, formatter);
                LocalDate currentDate = LocalDate.parse(newDate, formatter);
                return Period.between(lastDate, currentDate).getDays();
            } catch (Exception e) {
                LOG.error("Failed to calculate day difference between {} and {}", oldDate, newDate, e);
                throw new RuntimeException("Failed to calculate day difference", e);
            }
        }
    }

    private static DataStream<Tuple9<String, Double, Double, Double, Double, Double, String, Double, String>> performTransformations(
            DataStream<Tuple7<String, Double, Double, Double, Double, Double, String>> inputStream,
            long eventGap) {
        return inputStream
            .keyBy(value -> value.f0)
            .window(ProcessingTimeSessionWindows.withGap(Time.milliseconds(eventGap)))
            .process(new StockPriceProcessor());
    }

    public static DataStream<String> tuple9ToString(
            DataStream<Tuple9<String, Double, Double, Double, Double, Double, String, Double, String>> inputStream) {
        return inputStream.map(
            new MapFunction<Tuple9<String, Double, Double, Double, Double, Double, String, Double, String>, String>() {
                @Override
                public String map(
                        Tuple9<String, Double, Double, Double, Double, Double, String, Double, String> value) {
                    JsonObject jsonObject = new JsonObject();
                    jsonObject.addProperty("symbol", value.f0);
                    jsonObject.addProperty("close", value.f1);
                    jsonObject.addProperty("open", value.f2);
                    jsonObject.addProperty("low", value.f3);
                    jsonObject.addProperty("high", value.f4);
                    jsonObject.addProperty("volume", value.f5);
                    jsonObject.addProperty("timestamp", value.f6);
                    jsonObject.addProperty("%change", value.f7);
                    jsonObject.addProperty("indicator", value.f8);

                    return jsonObject.toString();
                }
            });
    }
}
