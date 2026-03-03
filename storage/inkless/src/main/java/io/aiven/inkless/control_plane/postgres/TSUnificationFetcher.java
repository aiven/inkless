package io.aiven.inkless.control_plane.postgres;

import io.aiven.inkless.cache.ObjectCache;
import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.consume.FetchCompleter;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.BatchMetadata;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;
import io.aiven.inkless.storage_backend.common.StorageBackendException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.utils.Time;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.generated.enums.FileStateT;
import org.jspecify.annotations.NonNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.jooq.generated.Tables.BATCHES;
import static org.jooq.generated.Tables.FILES;

public class TSUnificationFetcher implements Supplier<Map<TopicIdPartition, TreeMap<BatchInfo, MemoryRecords>>> {

    private final Time time;
    private final ObjectCache cache;
    private final ObjectFetcher objectFetcher;
    private final ObjectKeyCreator objectKeyCreator;
    private final int maxObjectAmount = 10; // limit the max amount of objects to fetch
    private final DSLContext jooqCtx;
    private Comparator<TopicIdPartition> topicIdPartitionComparator = Comparator
            .comparing(TopicIdPartition::topicId)
            .thenComparing(TopicIdPartition::partition);

    public TSUnificationFetcher(Time time, ObjectCache cache, ObjectFetcher objectFetcher, ObjectKeyCreator objectKeyCreator, DSLContext jooqCtx) {
        this.time = time;
        this.cache = cache;
        this.objectFetcher = objectFetcher;
        this.objectKeyCreator = objectKeyCreator;
        this.jooqCtx = jooqCtx;
    }

    @Override
    public Map<TopicIdPartition, TreeMap<BatchInfo, MemoryRecords>> get() {
        // 1. fetch the earliest next X object keys that aren't yet deleted
        var fileRows = new HashSet<>(jooqCtx
            .select(FILES.FILE_ID, FILES.OBJECT_KEY)
            .from(FILES)
            .where(FILES.STATE.ne(FileStateT.deleting))
            .orderBy(FILES.FILE_ID.asc())
            .limit(maxObjectAmount)
            .fetch());
        // 2. fetch the files from object storage
        var recordsMap = fileRows.stream()
            .map(obr -> {
                var objectKey = obr.get(FILES.OBJECT_KEY);
                var bis = batchInfos(obr);
                var batchMap = toBatchRecordMap(bis, objectKey);
                return byPartition(batchMap);
            });
        return combineBatchMaps(recordsMap);
        // TODO: discard batches that have been moved to object storage already
    }

    private @NonNull TreeMap<TopicIdPartition, TreeMap<BatchInfo, MemoryRecords>> combineBatchMaps(Stream<TreeMap<TopicIdPartition, TreeMap<BatchInfo, MemoryRecords>>> recordsMap) {
        return recordsMap
                .flatMap(m -> m.entrySet().stream())
                .collect(Collectors.toMap(
                        Map.Entry::getKey,
                        e -> new TreeMap<>(e.getValue()),
                        (a, b) -> {
                            TreeMap<BatchInfo, MemoryRecords> combined = new TreeMap<>(a);
                            combined.putAll(b);
                            return combined;
                        },
                        () -> new TreeMap<>(topicIdPartitionComparator)
                ));
    }

    private Set<BatchInfo> batchInfos(Record fileRow) {
        var bros = new HashSet<>(jooqCtx.select(BATCHES.asterisk())
            .from(BATCHES)
            .where(BATCHES.FILE_ID.eq(fileRow.get(FILES.FILE_ID)))
            .fetch());
        return bros.stream().map(bro -> {
            var batchMetadata = BatchMetadata.of(
                new TopicIdPartition(bro.get(BATCHES.TOPIC_ID), bro.get(BATCHES.PARTITION), null),
                bro.get(BATCHES.BYTE_OFFSET),
                bro.get(BATCHES.BYTE_SIZE),
                bro.get(BATCHES.BASE_OFFSET),
                bro.get(BATCHES.LAST_OFFSET),
                bro.get(BATCHES.LOG_APPEND_TIMESTAMP),
                bro.get(BATCHES.BATCH_MAX_TIMESTAMP),
                bro.get(BATCHES.TIMESTAMP_TYPE)
            );
            return new BatchInfo(bro.get(BATCHES.BATCH_ID), fileRow.get(FILES.OBJECT_KEY), batchMetadata);
        }).collect(Collectors.toSet());
    }

    private Map<BatchInfo, MemoryRecords> toBatchRecordMap(Set<BatchInfo> bis, String objectKey) {
        return bis.stream()
            .collect(Collectors.toMap(
                bi -> bi,
                bi -> {
                    try {
                        var fileExtent = createFileExtent(objectKeyCreator.from(objectKey), bi.metadata().range());
                        return FetchCompleter.constructRecordsFromFile(bi, Collections.singletonList(fileExtent));
                    } catch (IOException | StorageBackendException e) {
                        throw new RuntimeException(e);
                    }
                }
            ));
    }

    private static TreeMap<TopicIdPartition, TreeMap<BatchInfo, MemoryRecords>> byPartition(Map<BatchInfo, MemoryRecords> batchMap) {
        return batchMap
            .entrySet()
            .stream()
            .collect(Collectors.groupingBy(
                e -> e.getKey().metadata().topicIdPartition(),
                TreeMap::new,
                Collectors.toMap(
                    Map.Entry::getKey,
                    Map.Entry::getValue,
                    (a, b) -> a,
                    () -> new TreeMap<>(Comparator.comparingLong(bi -> bi.metadata().baseOffset()))
                )
            ));
    }

    // visible for testing
    private FileExtent createFileExtent(ObjectKey object, ByteRange byteRange)
            throws IOException, StorageBackendException {
        final ByteBuffer byteBuffer = objectFetcher.readToByteBuffer(objectFetcher.fetch(object, byteRange));
        return new FileExtent()
            .setObject(object.value())
            .setRange(new FileExtent.ByteRange()
                .setOffset(byteRange.offset())
                .setLength(byteBuffer.limit()))
            .setData(byteBuffer.array());
    }
}
