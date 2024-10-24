package io.aiven.cps.common;

import java.util.List;

public class CommitFileResponse {
    final List<Long> assignedOffsets;

    public CommitFileResponse(final List<Long> assignedOffsets) {
        this.assignedOffsets = assignedOffsets;
    }
}
