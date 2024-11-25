// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.test_utils;

public class PostgreSQLTestContainer {
    public static final String USERNAME = "test";
    public static final String PASSWORD = "test";

    public static PostgreSQLContainer container() {
        return new PostgreSQLContainer("postgres:17.2")
            .withUsername(USERNAME)
            .withUsername(PASSWORD);
    }
}
