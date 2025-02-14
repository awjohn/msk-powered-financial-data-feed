package com.amazonaws.services.msf;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Period;
import java.time.format.DateTimeFormatter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonObject;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
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

    /**
     * Map input stream to tuple stream
     */
    private static DataStream<Tuple7<String, Double, Double, Double, Double, Double, String>> mapToTupleStream(
            DataStream<String> inputStream) {
        return inputStream
            .map(new MapFunction<String, Tuple7<String, Double, Double, Double, Double, Double, String>>() {
                @Override
                public Tuple7<String, Double, Double, Double, Double, Double, String> map(String value)
                        throws Exception {
                    LOG.info("Processing Kafka record: {}", value);
                    ObjectMapper mapper = new ObjectMapper();
                    JsonNode node = mapper.readTree(value);

                    Double lowPrice = node.get("low").asDouble();
                    Double volume = node.get("volume").asDouble();
                    Double highPrice = node.get("high").asDouble();
                    Double openPrice = node.get("open").asDouble();
                    Double closePrice = node.get("close").asDouble();
                    String stockSymbol = node.get("symbol").asText();
                    String eventTime = node.get("timestamp").asText();

                    LOG.debug("Mapped to tuple: [{}, {}, {}, {}, {}, {}, {}]",
                            stockSymbol, closePrice, openPrice, lowPrice, highPrice, volume, eventTime);
                    return new Tuple7<>(stockSymbol, closePrice, openPrice, lowPrice,
                            highPrice, volume, eventTime);
                }
            });
    }

    /**
     * Convert tuple stream to string stream
     */
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

    /**
     * Perform transformations on the tuple stream
     */
    private static DataStream<Tuple9<String, Double, Double, Double, Double, Double, String, Double, String>> performTransformations(
            DataStream<Tuple7<String, Double, Double, Double, Double, Double, String>> inputStream,
            long eventGap) {
        return inputStream
            .keyBy(value -> value.f0)
            .window(ProcessingTimeSessionWindows.withGap(Time.milliseconds(eventGap)))
            .process(new StockPriceProcessor());
    }

    /**
     * Processor for calculating stock price changes
     */
    private static class StockPriceProcessor extends ProcessWindowFunction<
            Tuple7<String, Double, Double, Double, Double, Double, String>,
            Tuple9<String, Double, Double, Double, Double, Double, String, Double, String>,
            String,
            TimeWindow> {
        private ValueState<Tuple7<String, Double, Double, Double, Double, Double, String>> lastDayObjectState;
        private ValueState<Tuple7<String, Double, Double, Double, Double, Double, String>> lastProcessedObjectState;

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<Tuple7<String, Double, Double, Double, Double, Double, String>> lastProcessedDescriptor = 
                new ValueStateDescriptor<>(
                    "lastProcessedObjectState",
                    TypeInformation.of(
                        new TypeHint<Tuple7<String, Double, Double, Double, Double, Double, String>>() {}
                    )
                );
            lastProcessedObjectState = getRuntimeContext().getState(lastProcessedDescriptor);

            ValueStateDescriptor<Tuple7<String, Double, Double, Double, Double, Double, String>> lastDayDescriptor = 
                new ValueStateDescriptor<>(
                    "lastDayObjectState",
                    TypeInformation.of(
                        new TypeHint<Tuple7<String, Double, Double, Double, Double, Double, String>>() {}
                    )
                );
            lastDayObjectState = getRuntimeContext().getState(lastDayDescriptor);
        }

        @Override
        public void process(
                String key,
                Context context,
                Iterable<Tuple7<String, Double, Double, Double, Double, Double, String>> records,
                Collector<Tuple9<String, Double, Double, Double, Double, Double, String, Double, String>> out)
                throws Exception {
            LOG.info("Processing Session window for key: {}", key);

            Tuple7<String, Double, Double, Double, Double, Double, String> lastDayObject = lastDayObjectState.value();
            Tuple7<String, Double, Double, Double, Double, Double, String> lastProcessedObject = lastProcessedObjectState.value();

            for (Tuple7<String, Double, Double, Double, Double, Double, String> record : records) {
                processRecord(record, lastDayObject, lastProcessedObject, out);
                updateState(record, lastProcessedObject);
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
            if (lastProcessedObject != null) {
                Integer dayDifference = calculateDayDifference(lastProcessedObject.f6, record.f6);
                if (dayDifference >= 1) {
                    LOG.info("Updating state for new date");
                    lastDayObjectState.update(lastProcessedObject);
                }
            }
            lastProcessedObjectState.update(record);
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
}
