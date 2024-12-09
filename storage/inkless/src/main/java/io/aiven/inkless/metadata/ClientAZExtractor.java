// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.metadata;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

class ClientAZExtractor {
    private static final Pattern CLIENT_AZ_PATTERN = Pattern.compile("(^|[ ,])inkless_az=(?<az>[^=, ]*)");

    // Visible for testing
    static String getClientAZ(final String clientId) {
        if (clientId == null) {
            return null;
        }

        final Matcher matcher = CLIENT_AZ_PATTERN.matcher(clientId);
        if (!matcher.find()) {
            return null;
        }
        return matcher.group("az");
    }
}
