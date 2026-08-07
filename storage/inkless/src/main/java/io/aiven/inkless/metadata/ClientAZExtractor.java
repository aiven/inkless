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

import java.util.Set;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

class ClientAZExtractor {
    private static final Pattern CLIENT_AZ_PATTERN = Pattern.compile("(^|[ ,])diskless_az=(?<az>[^=, ]*)");

    // Visible for testing
    static String getClientAZ(final String clientId) {
        return getClientAZ(clientId, Set::of);
    }

    /**
     * Extract the client AZ from the client ID, validating against known rack values if provided.
     *
     * @param clientId the client ID string, may be null
     * @param racksSupplier supplier of valid rack/AZ identifiers, evaluated when needed.
     *                      An empty set disables validation.
     * @return the extracted AZ, or null if not found or not matchable
     */
    static String getClientAZ(final String clientId, final Supplier<Set<String>> racksSupplier) {
        if (clientId == null) {
            return null;
        }

        final Matcher matcher = CLIENT_AZ_PATTERN.matcher(clientId);
        if (!matcher.find()) {
            return null;
        }

        final String rawAZ = matcher.group("az");
        if (rawAZ == null || rawAZ.isEmpty()) {
            return rawAZ;
        }

        final Set<String> racks = racksSupplier.get();
        if (racks == null || racks.isEmpty()) {
            return rawAZ;
        }

        if (racks.contains(rawAZ)) {
            return rawAZ;
        }

        String azCandidate = rawAZ;
        int lastHyphen;
        while ((lastHyphen = azCandidate.lastIndexOf('-')) > 0) {
            azCandidate = azCandidate.substring(0, lastHyphen);
            if (racks.contains(azCandidate)) {
                return azCandidate;
            }
        }

        return null;
    }
}
