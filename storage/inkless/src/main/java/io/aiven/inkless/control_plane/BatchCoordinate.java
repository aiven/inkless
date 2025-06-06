package io.aiven.inkless.control_plane;

import org.apache.kafka.common.TopicIdPartition;

import java.util.Objects;

public abstract class BatchCoordinate {
    abstract TopicIdPartition topicIdPartition();

    public abstract String objectKey();

    public abstract long byteOffset();

    public abstract long byteSize();

    public abstract long baseOffset();

    public abstract long lastOffset();

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof BatchCoordinate other)) {
            return false;
        }
        return this.byteOffset() == other.byteOffset() &&
            this.byteSize() == other.byteSize() &&
            this.baseOffset() == other.baseOffset() &&
            this.lastOffset() == other.lastOffset() &&
            Objects.equals(this.topicIdPartition(), other.topicIdPartition()) &&
            Objects.equals(this.objectKey(), other.objectKey());
    }

    @Override
    public int hashCode() {
        return Objects.hash(topicIdPartition(), objectKey(), byteOffset(), byteSize(), baseOffset(), lastOffset());
    }
}
