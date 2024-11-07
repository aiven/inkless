// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.storage_backend.common.fixtures;

import org.junit.jupiter.api.TestInfo;

public class TestUtils {
    public static String testNameToBucketName(final TestInfo testInfo) {
        String bucketName = testInfo.getDisplayName()
            .toLowerCase()
            .replace(" ", "")
            .replace(",", "-")
            .replace("(", "")
            .replace(")", "")
            .replace("[", "")
            .replace("]", "");
        while (bucketName.length() < 3) {
            bucketName += bucketName;
        }
        return bucketName;
    }
}
