/*
 * Inkless
 * Copyright (C) 2024 - 2025 Aiven OY
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package io.aiven.inkless.metadata;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

class ClientAZExtractor {
    private static final Pattern CLIENT_AZ_PATTERN = Pattern.compile("(^|[ ,])diskless_az=(?<az>[^=, ]*)");

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
