package com.skywaet.storm.minio;

import io.minio.*;
import io.minio.errors.*;
import io.minio.messages.Item;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;

import static java.util.Objects.requireNonNull;

public class MinioClient {
    @Nonnull
    private final io.minio.MinioClient client;

    public MinioClient(@Nonnull MinioClientProperties properties) {
        requireNonNull(properties);
        this.client = io.minio.MinioClient.builder()
                .endpoint(properties.getEndpoint())
                .credentials(properties.getUsername(), properties.getPassword())
                .build();
    }

    public void makeBucket(String bucketName) {
        try {
            client.makeBucket(
                    MakeBucketArgs.builder()
                            .bucket(bucketName)
                            .build()
            );
        } catch (ErrorResponseException | InsufficientDataException | InternalException | InvalidKeyException |
                 InvalidResponseException | IOException | NoSuchAlgorithmException | ServerException |
                 XmlParserException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean bucketExists(String bucketName) {
        try {
            return client.bucketExists(BucketExistsArgs.builder()
                    .bucket(bucketName)
                    .build());
        } catch (ErrorResponseException | InsufficientDataException | InternalException | InvalidKeyException |
                 InvalidResponseException | IOException | NoSuchAlgorithmException | ServerException |
                 XmlParserException e) {
            throw new RuntimeException(e);
        }
    }

    public void putData(String bucketName, String fileName, InputStream content) {
        try {
            client.putObject(
                    PutObjectArgs.builder()
                            .bucket(bucketName)
                            .object(fileName)
                            .stream(content, -1, 5 * 1024 * 1024)
                            .build()
            );
        } catch (ErrorResponseException | InsufficientDataException | InternalException | InvalidKeyException |
                 InvalidResponseException | IOException | NoSuchAlgorithmException | ServerException |
                 XmlParserException e) {
            throw new RuntimeException(e);
        }
    }

    public Iterable<Result<Item>> listFiles(String bucketName) {
        return client.listObjects(ListObjectsArgs.builder()
                .bucket(bucketName)
                .build());
    }

    public GetObjectResponse readFile(String bucketName, String fileName) {
        try {
            return client.getObject(GetObjectArgs.builder()
                    .bucket(bucketName)
                    .object(fileName)
                    .build());
        } catch (ErrorResponseException | InsufficientDataException | InternalException | InvalidKeyException |
                 InvalidResponseException | IOException | NoSuchAlgorithmException | ServerException |
                 XmlParserException e) {
            throw new RuntimeException(e);
        }
    }

    public boolean objectExists(String bucketName, String fileName) {
        try {
            client.statObject(StatObjectArgs.builder()
                    .bucket(bucketName)
                    .object(fileName).build());
            return true;
        } catch (ErrorResponseException e) {
            return false;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

