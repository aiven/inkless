// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

/**
 * The reasons why a file on the remote storage exists.
 */
public enum FileReason {
    /**
     * Uploaded by a broker as the result of producing.
     */
    PRODUCE("produce"),

    /**
     * Uploaded by a broker as the result of merging.
     */
    MERGE("merge");

    public final String name;

    FileReason(final String name) {
        this.name = name;
    }

    public static FileReason fromName(final String name) {
        if (PRODUCE.name.equals(name)) {
            return PRODUCE;
        } else if (MERGE.name.equals(name)) {
            return MERGE;
        } else {
            throw new IllegalArgumentException("Unknown name " + name);
        }
    }
}
