package com.skywaet.storm.bolt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.regex.Pattern;

public class LogLevelFilteringBolt extends BaseRichBolt {
    private static final Logger log = LoggerFactory.getLogger(LogLevelFilteringBolt.class);
    private static final Pattern LOG_MESSAGE = Pattern.compile("((?<dateTime>\\d{4}-\\d{2}-\\d{2} (\\d+|:?)+) \\d+ (?<level>[A-Z]+).+)");
    private OutputCollector collector;
    private ObjectMapper mapper;

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        this.collector = collector;
        this.mapper = new ObjectMapper();
    }

    @Override
    public void execute(Tuple input) {
        var message = input.getStringByField("value");
        try {
            var node = mapper.readTree(message);
            var level = node.get("log.level").asText();
            if ("WARN".equals(level) || "ERROR".equals(level)) {
                log.info("WARN or ERROR log message found: {}", message);
                var timestamp = node.get("@timestamp").asText();
                collector.emit(new Values(timestamp, level));
            }
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("timestamp", "level"));
    }
}
