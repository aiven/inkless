// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.test_utils;

import org.testcontainers.containers.wait.strategy.Wait;

public class PostgreSQLTestContainer {
    public static final String USERNAME = "test";
    public static final String PASSWORD = "test";

    public static InklessPostgreSQLContainer container() {
        return new InklessPostgreSQLContainer("postgres:17.2")
            .withUsername(USERNAME)
            .withUsername(PASSWORD)
            .waitingFor(Wait.forListeningPort());
    }
}
