package kafka.server.inkless_reader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.record.FileRecords;
import org.apache.kafka.common.record.Records;
import org.apache.kafka.common.requests.FetchRequest;
import org.apache.kafka.server.storage.log.FetchPartitionData;

import kafka.server.inkless_common.FindBatchRequest;
import kafka.server.inkless_common.FindBatchResponse;
import kafka.server.inkless_control_plane.ControlPlane;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

public class InklessReader {
    private static final Logger logger = LoggerFactory.getLogger(InklessReader.class);

    private final ControlPlane controlPlane;

    public InklessReader(final ControlPlane controlPlane) {
        this.controlPlane = controlPlane;
    }

    public CompletableFuture<Iterable<Tuple2<TopicIdPartition, FetchPartitionData>>> read(
        final Iterable<Tuple2<TopicIdPartition, FetchRequest.PartitionData>> fetchInfos
    ) {
        final List<Tuple2<TopicIdPartition, FetchPartitionData>> result = new ArrayList<>();

        for (final Tuple2<TopicIdPartition, FetchRequest.PartitionData> fetchInfo : fetchInfos) {
            FindBatchResponse findBatchResponse = controlPlane.findBatch(new FindBatchRequest(
                fetchInfo._1.topicPartition(),
                fetchInfo._2.fetchOffset
            ));
            logger.error("findBatchResponse={}", findBatchResponse);

            final Records records;
            try {
                records = FileRecords.open(new File(findBatchResponse.filePath))
                    .slice(findBatchResponse.byteOffset, findBatchResponse.byteSize);
            } catch (final IOException e) {
                // TODO handle
                throw new RuntimeException(e);
            }

            final FetchPartitionData fetchPartitionData = new FetchPartitionData(
                Errors.NONE,
                findBatchResponse.batchBaseOffset + findBatchResponse.numberOfRecords,
                findBatchResponse.batchBaseOffset,
                records,
                Optional.empty(),
                OptionalLong.of(findBatchResponse.batchBaseOffset + findBatchResponse.numberOfRecords),
                Optional.empty(),
                OptionalInt.empty(),
                false
            );
            result.add(new Tuple2<>(fetchInfo._1, fetchPartitionData));
        }

        return CompletableFuture.completedFuture(result);
    }
}
