// Copyright (c) 2024 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

public enum FileState {
    /**
     * Uploaded by a broker, in use, etc.
     */
    UPLOADED("uploaded"),
    /**
     * Marked for deletion.
     */
    DELETING("deleting");

    public final String name;

    FileState(final String name) {
        this.name = name;
    }

    public static FileState fromName(final String name) {
        if (UPLOADED.name.equals(name)) {
            return UPLOADED;
        } else if (DELETING.name.equals(name)) {
            return DELETING;
        } else {
            throw new IllegalArgumentException("Unknown name " + name);
        }
    }
}
