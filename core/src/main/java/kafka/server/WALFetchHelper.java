package kafka.server;

import io.aiven.inkless.common.SharedState;

public class WALFetchHelper {

    private final SharedState sharedState;

    public WALFetchHelper(SharedState sharedState) {
        this.sharedState = sharedState;
    }
}
