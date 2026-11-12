import java.util.OptionalLong;

public class RemoteStorageFetchInfo {

    public final int fetchMaxBytes;
    public final boolean minOneMessage;
    public final TopicPartition topicPartition;
    public final FetchRequest.PartitionData fetchInfo;
    public final FetchIsolation fetchIsolation;
    /**
     * The exclusive offset where the read ends. It holds the classic-to-diskless seal of a switched partition,
     * and it is empty for every other partition.
     */
    public final OptionalLong upperBoundOffset;

    public RemoteStorageFetchInfo(int fetchMaxBytes, boolean minOneMessage, TopicPartition topicPartition,
                                  FetchRequest.PartitionData fetchInfo, FetchIsolation fetchIsolation) {
        this(fetchMaxBytes, minOneMessage, topicPartition, fetchInfo, fetchIsolation, OptionalLong.empty());
    }

    public RemoteStorageFetchInfo(int fetchMaxBytes, boolean minOneMessage, TopicPartition topicPartition,
                                  FetchRequest.PartitionData fetchInfo, FetchIsolation fetchIsolation,
                                  OptionalLong upperBoundOffset) {
        this.fetchMaxBytes = fetchMaxBytes;
        this.minOneMessage = minOneMessage;
        this.topicPartition = topicPartition;
        this.fetchInfo = fetchInfo;
        this.fetchIsolation = fetchIsolation;
        this.upperBoundOffset = upperBoundOffset;
    }

    @Override
    public String toString() {
        return "RemoteStorageFetchInfo{" +
                "fetchMaxBytes=" + fetchMaxBytes +
                ", minOneMessage=" + minOneMessage +
                ", topicPartition=" + topicPartition +
                ", fetchInfo=" + fetchInfo +
                ", fetchIsolation=" + fetchIsolation +
                ", upperBoundOffset=" + upperBoundOffset +
                '}';
    }
}
