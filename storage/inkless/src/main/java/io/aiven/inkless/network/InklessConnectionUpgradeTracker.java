// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.network;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

import io.aiven.inkless.control_plane.MetadataView;

/**
 * Tracks whether a connection is Inkless-upgraded.
 *
 * <p>For each upgraded connection, remembers the correlation ID at the moment of upgrade (used later for response queueing).
 */
public class InklessConnectionUpgradeTracker {
    private static final Logger LOGGER = LoggerFactory.getLogger(InklessConnectionUpgradeTracker.class);

    private final MetadataView metadataView;
    // Key: connection ID, value: correlation ID at the moment of upgrade
    private final ConcurrentHashMap<String, Integer> upgradedConnection = new ConcurrentHashMap<>();

    public InklessConnectionUpgradeTracker(final MetadataView metadataView) {
        this.metadataView = metadataView;
    }

    public boolean isConnectionUpgraded(final String connectionId) {
        return upgradedConnection.containsKey(connectionId);
    }

    public int upgradeCorrelationId(final String connectionId) {
        final Integer result = upgradedConnection.get(connectionId);
        if (result != null) {
            return result;
        } else {
            throw new IllegalStateException();
        }
    }

    public void upgradeConnection(final String connectionId, final int correlationId) {
        upgradedConnection.computeIfAbsent(connectionId, ignore -> {
            LOGGER.info("Upgrading connection {}", connectionId);
            return correlationId;
        });
    }

    public void closeConnection(final String connectionId) {
        // TODO call
        upgradedConnection.remove(connectionId);
    }
}
