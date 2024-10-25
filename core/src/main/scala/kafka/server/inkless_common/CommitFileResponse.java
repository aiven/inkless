package kafka.server.inkless_common;

import java.util.List;

public class CommitFileResponse {
    public final List<Long> assignedOffsets;

    public CommitFileResponse(final List<Long> assignedOffsets) {
        this.assignedOffsets = assignedOffsets;
    }
}
