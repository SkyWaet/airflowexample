package com.skywaet.storm.minio;


import javax.annotation.Nonnull;
import java.io.Serializable;

import static java.util.Objects.requireNonNull;

public class MinioClientProperties implements Serializable {
    @Nonnull
    private final String username;
    @Nonnull
    private final String password;
    @Nonnull
    private final String endpoint;

    public MinioClientProperties(@Nonnull String username,
                                 @Nonnull String password,
                                 @Nonnull String endpoint) {
        this.username = requireNonNull(username);
        this.password = requireNonNull(password);
        this.endpoint = requireNonNull(endpoint);
    }


    @Nonnull
    public String getUsername() {
        return username;
    }

    @Nonnull
    public String getPassword() {
        return password;
    }

    @Nonnull
    public String getEndpoint() {
        return endpoint;
    }
}
