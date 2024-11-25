// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.test_utils;

public class PostgreSQLContainer extends org.testcontainers.containers.PostgreSQLContainer<PostgreSQLContainer> {
    public PostgreSQLContainer(final String dockerImageName) {
        super(dockerImageName);
    }

    public String getJdbcUrl(final String databaseName) {
        String additionalUrlParams = constructUrlParameters("?", "&");
        return (
            "jdbc:postgresql://" +
                getHost() +
                ":" +
                getMappedPort(POSTGRESQL_PORT) +
                "/" +
                databaseName +
                additionalUrlParams
        );
    }
}
