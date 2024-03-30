package com.skywaet.storm;

import com.skywaet.storm.bolt.LogLevelFilteringBolt;
import com.skywaet.storm.bolt.MessageCountingBolt;
import com.skywaet.storm.bolt.MinioWritingBolt;
import com.skywaet.storm.minio.MinioClientProperties;
import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseWindowedBolt;

import java.util.UUID;

public class Main {
    public static void main(String[] args) throws AuthorizationException, InvalidTopologyException, AlreadyAliveException {
        TopologyBuilder tp = new TopologyBuilder();
        var kafkaSpoutConfig = KafkaSpoutConfig
                .builder("kafka:9093", "logs")
                .setGroupId(UUID.randomUUID().toString())
                .setProcessingGuarantee(KafkaSpoutConfig.ProcessingGuarantee.AT_MOST_ONCE)
                .build();
        tp.setSpout("kafka_spout", new KafkaSpout<>(kafkaSpoutConfig), 1);
        tp.setBolt("filtering_bolt", new LogLevelFilteringBolt())
                .shuffleGrouping("kafka_spout");

        var countingBolt = new MessageCountingBolt()
                .withLag(BaseWindowedBolt.Duration.of(100))
                .withWindow(BaseWindowedBolt.Duration.minutes(1));

        tp.setBolt("counting_bolt", countingBolt)
                .shuffleGrouping("filtering_bolt");

        var minioProperties = new MinioClientProperties(
                "minioadmin",
                "minioadmin",
                "http://minio:9000"
        );
        var minioBolt = new MinioWritingBolt("logcount", minioProperties);
        tp.setBolt("writing_bolt", minioBolt)
                .shuffleGrouping("counting_bolt");

        var config = new Config();
        config.setDebug(false);
        config.setMessageTimeoutSecs(60);
        StormSubmitter.submitTopology("logcount", config, tp.createTopology());
    }
}
