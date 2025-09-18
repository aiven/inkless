package org.apache.kafka.common.requests;

import org.apache.kafka.common.message.InklessCommitResponseData;
import org.apache.kafka.common.protocol.ApiKeys;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.protocol.Readable;

import java.util.Collections;
import java.util.Map;

public class InklessCommitResponse extends AbstractResponse {
    private final InklessCommitResponseData data;

    public InklessCommitResponse(InklessCommitResponseData data) {
        super(ApiKeys.INKLESS_COMMIT);
        this.data = data;
    }

    @Override
    public InklessCommitResponseData data() {
        return data;
    }

    @Override
    public Map<Errors, Integer> errorCounts() {
        if (data.errorCode() != Errors.NONE.code()) {
            return Collections.singletonMap(Errors.forCode(data.errorCode()), 1);
        } else {
            return Collections.emptyMap();
        }
    }

    @Override
    public int throttleTimeMs() {
        return data.throttleTimeMs();
    }

    @Override
    public void maybeSetThrottleTimeMs(final int throttleTimeMs) {
        data.setThrottleTimeMs(throttleTimeMs);
    }

    public static InklessCommitResponse parse(Readable readable, short version) {
        return new InklessCommitResponse(
            new InklessCommitResponseData(readable, version));
    }
}
