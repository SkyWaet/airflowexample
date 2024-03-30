package com.skywaet;

import io.minio.MinioClient;
import io.minio.PutObjectArgs;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Consumer {

    private static final Logger log = LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) throws IOException, URISyntaxException {
        Properties consumerProperties = new Properties();
        consumerProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        var tempDir = Files.createDirectories(Paths.get(Consumer.class.getResource("/buffer").toURI()).resolve(
                "consumer-test"));

        MinioClient minioClient =
                MinioClient.builder()
                        .endpoint("http://127.0.0.1:9000")
                        .credentials("minioadmin", "minioadmin")
                        .build();
        var executor = Executors.newSingleThreadScheduledExecutor();

        executor.scheduleAtFixedRate(() -> {
            var fileName = getFileName(ZonedDateTime.now().minusMinutes(2));
            var filePath = tempDir.resolve(fileName);
            if (!Files.exists(filePath)) {
                log.warn("File {} does not exist", filePath);
                return;
            }
            try (var stream = new FileInputStream(filePath.toFile())) {
                log.info("Uploading logs");
                minioClient.putObject(PutObjectArgs.builder()
                        .stream(stream, -1, 5 * 1024 * 1024)
                        .object(fileName)
                        .bucket("logs")
                        .build());
            } catch (Exception e) {
                log.error("Error while uploading logs", e);
                throw new RuntimeException(e);
            }
            try {
                Files.deleteIfExists(filePath);
            } catch (IOException e) {
                log.error("Error while deleting file", e);
            }
        }, 2, 1, TimeUnit.MINUTES);

        log.info(tempDir.toString());
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProperties)) {
            consumer.subscribe(List.of("logs"));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
                var file = tempDir.resolve(getFileName(ZonedDateTime.now()));
                try (var writer = new FileWriter(file.toFile(), true)) {
                    for (ConsumerRecord<String, String> record : records) {
                        log.info("Processing message {}", record.value());
                        writer.write(record.value());
                    }
                }
            }
        }
    }

    private static String getFileName(ZonedDateTime time) {
        return "timestamp-" + time.truncatedTo(ChronoUnit.MINUTES).toEpochSecond();
    }
}