package com.skywaet.storm.bolt;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseWindowedBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.windowing.TupleWindow;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class MessageCountingBolt extends BaseWindowedBolt {
    private OutputCollector outputCollector;
    private ObjectMapper mapper;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        this.outputCollector = collector;
        this.mapper = new ObjectMapper();
    }


    @Override
    public void execute(TupleWindow inputWindow) {
        var tuples = inputWindow.get();

        tuples.stream()
                .collect(Collectors.toMap(this::extractTimestamp, this::toMessageMap, this::merge))
                .entrySet()
                .stream()
                .map(entry -> new Result(entry.getKey().format(DateTimeFormatter.ofPattern("yyyy-MM-dd--HH-mm")),
                        entry.getValue().getOrDefault("WARN", 0L),
                        entry.getValue().getOrDefault("ERROR", 0L)))
                .map(this::serialize)
                .map(Values::new)
                .forEach(outputCollector::emit);
    }

    private LocalDateTime extractTimestamp(Tuple tuple) {
        return LocalDateTime.parse(tuple.getStringByField("timestamp"), DateTimeFormatter.ofPattern("yyyy-MM-dd--HH-mm-ss"))
                .truncatedTo(ChronoUnit.MINUTES);
    }

    private Map<String, Long> toMessageMap(Tuple tuple) {
        var level = tuple.getStringByField("level");
        return Map.of(level, 1L);
    }

    private Map<String, Long> merge(Map<String, Long> oldValue, Map<String, Long> newValue) {
        var result = new HashMap<>(oldValue);
        newValue.forEach((level, count) -> {
            result.compute(level, (k, v) -> v == null ? count : v + count);
        });
        return result;
    }

    private String serialize(Result result) {
        try {
            return mapper.writeValueAsString(result);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("result"));
    }

    public static class Result {
        private final String timestamp;
        private final Long warnLogs;
        private final Long errorLogs;

        @JsonCreator
        public Result(@JsonProperty("timestamp") String timestamp,
                      @JsonProperty("warnLogs") Long warnLogs,
                      @JsonProperty("errorLogs") Long errorLogs) {
            this.timestamp = timestamp;
            this.warnLogs = warnLogs;
            this.errorLogs = errorLogs;
        }

        @JsonProperty
        public String getTimestamp() {
            return timestamp;
        }

        @JsonProperty
        public Long getWarnLogs() {
            return warnLogs;
        }

        @JsonProperty
        public Long getErrorLogs() {
            return errorLogs;
        }
    }
}
