package com.skywaet.storm.bolt;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.skywaet.storm.minio.MinioClient;
import com.skywaet.storm.minio.MinioClientProperties;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Tuple;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class MinioWritingBolt extends BaseRichBolt {
    private final String bucketName;
    private final MinioClientProperties minioClientProperties;
    private ObjectMapper mapper;

    private MinioClient client;

    public MinioWritingBolt(String bucketName,
                            MinioClientProperties minioClientProperties) {
        this.bucketName = bucketName;
        this.minioClientProperties = minioClientProperties;
    }

    @Override
    public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector collector) {
        client = new MinioClient(minioClientProperties);
        mapper = new ObjectMapper();
    }

    @Override
    public void execute(Tuple input) {
        if (!client.bucketExists(bucketName)) {
            client.makeBucket(bucketName);
        }
        var rawResult = input.getStringByField("result");
        var result = deserialize(rawResult);
        var message = String.format("WARN logs = %d\nERROR logs = %d\n", result.getErrorLogs(), result.getWarnLogs());
        client.putData(bucketName,
                result.getTimestamp(),
                new ByteArrayInputStream(message.getBytes(StandardCharsets.UTF_8)));
    }

    private MessageCountingBolt.Result deserialize(String rawResult) {
        try {
            return mapper.readValue(rawResult, MessageCountingBolt.Result.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    }

}
