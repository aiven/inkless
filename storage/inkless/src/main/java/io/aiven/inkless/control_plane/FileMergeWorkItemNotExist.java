// Copyright (c) 2025 Aiven, Helsinki, Finland. https://aiven.io/
package io.aiven.inkless.control_plane;

public class FileMergeWorkItemNotExist extends ControlPlaneException {
    public final long workItemId;

    public FileMergeWorkItemNotExist(final long workItemId) {
        super(String.format("Work item %d doesn't exist", workItemId));
        this.workItemId = workItemId;
    }
}
