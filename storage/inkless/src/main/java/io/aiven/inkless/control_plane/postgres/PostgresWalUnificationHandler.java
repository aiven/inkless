package io.aiven.inkless.control_plane.postgres;

import io.aiven.inkless.common.ByteRange;
import io.aiven.inkless.common.ObjectKey;
import io.aiven.inkless.common.ObjectKeyCreator;
import io.aiven.inkless.consume.FetchCompleter;
import io.aiven.inkless.control_plane.BatchInfo;
import io.aiven.inkless.control_plane.BatchMetadata;
import io.aiven.inkless.control_plane.WalUnificationHandler;
import io.aiven.inkless.generated.FileExtent;
import io.aiven.inkless.storage_backend.common.ObjectFetcher;
import io.aiven.inkless.storage_backend.common.StorageBackendException;
import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.record.MemoryRecords;
import org.jooq.Configuration;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.generated.Routines;
import org.jooq.generated.enums.FileStateT;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.jooq.generated.Tables.BATCHES;
import static org.jooq.generated.Tables.FILES;
import static org.jooq.impl.DSL.row;

public class PostgresWalUnificationHandler implements WalUnificationHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresWalUnificationHandler.class);
    private static final long NEXT_ID_SENTINEL = -1L;

    private final ObjectFetcher objectFetcher;
    private final ObjectKeyCreator objectKeyCreator;
    private final int maxObjectAmount = 10; // limit the max amount of objects to fetch
    private final DSLContext readJooqContext;
    private final DSLContext writeJooqContext;
    private Comparator<TopicIdPartition> topicIdPartitionComparator = Comparator
        .comparing(TopicIdPartition::topicId)
        .thenComparing(TopicIdPartition::partition);
    private Map<TopicIdPartition, Long> lastOffsets;
    private long nextFileIdToFetch = -1;
    private boolean initialized = false;
    private Map<TopicIdPartition, Long> remoteLogEndOffsets;

    public PostgresWalUnificationHandler(ObjectFetcher objectFetcher, ObjectKeyCreator objectKeyCreator,
                                         DSLContext readJooqContext, DSLContext writeJooqContext) {
        this.objectFetcher = objectFetcher;
        this.objectKeyCreator = objectKeyCreator;
        this.readJooqContext = readJooqContext;
        this.writeJooqContext = writeJooqContext;
    }

    @Override
    public Map<TopicIdPartition, TreeMap<BatchInfo, MemoryRecords>> apply(Set<TopicIdPartition> topicIdPartitions) {
        markWalFilesToDelete();
        // fetch the earliest next X object keys that aren't yet deleted
        LOGGER.trace("Current file to fetch: {} (amount: {})", nextFileIdToFetch, maxObjectAmount);
        var partitionRows = topicIdPartitions.stream()
            .map(tip -> row(tip.topicId(), tip.partition()))
            .toList();

        var fileRows = readJooqContext
            .selectDistinct(FILES.FILE_ID, FILES.OBJECT_KEY)
            .from(FILES)
            .innerJoin(BATCHES).on(BATCHES.FILE_ID.eq(FILES.FILE_ID))
            .where(row(BATCHES.TOPIC_ID, BATCHES.PARTITION).in(partitionRows))
            .and(FILES.STATE.ne(FileStateT.deleting))
            .and(FILES.FILE_ID.ge(nextFileIdToFetch))
            .orderBy(FILES.FILE_ID.asc())
            .limit(maxObjectAmount)
            .fetch();

        // update the next files to fetch for the next loop
        if (!fileRows.isEmpty()) {
            long maxId = fileRows.stream()
                .mapToLong(r -> r.get(FILES.FILE_ID))
                .max()
                .orElse(nextFileIdToFetch);
            nextFileIdToFetch = maxId + 1;
        }

        // fetch the files from object storage
        var recordsMap = fileRows.stream()
            .map(obr -> {
                var objectKey = obr.get(FILES.OBJECT_KEY);
                var bis = batchInfos(obr);
                var batchMap = toBatchRecordMap(bis, objectKey);
                return byPartition(batchMap);
            });
        var combinedBatchMap = combineBatchMaps(recordsMap);
        var combinedToString = combinedBatchMap.entrySet().stream().map(entry -> {
            String valueData = entry.getValue().entrySet().stream()
                .map(e -> String.valueOf(e.getKey().metadata().baseOffset()))
                .collect(Collectors.joining(", "));
            return "[" + entry.getKey() + ": " + valueData + "]";
        }).collect(Collectors.joining(", "));
        LOGGER.trace("Fetched batches:\n{}", combinedToString);
        LOGGER.trace("Next file to fetch: {} (amount: {})", nextFileIdToFetch, maxObjectAmount);
        return combinedBatchMap;
        // TODO: discard batches that have been moved to object storage already
    }

    private void initialize() {
        // fetch the earliest file not fully transformed
        if (lastOffsets != null && !initialized) {
            // TODO: optimize this query
            var tpToFileMap = lastOffsets.entrySet().stream().collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> {
                    var result = readJooqContext.select(BATCHES.FILE_ID)
                        .from(BATCHES)
                        .where(BATCHES.BASE_OFFSET.ge(entry.getValue()))
                        .and(BATCHES.TOPIC_ID.eq(entry.getKey().topicId()))
                        .and(BATCHES.PARTITION.eq(entry.getKey().partition()))
                        .orderBy(BATCHES.FILE_ID.asc())
                        .limit(1)
                        .fetch();
                    var record = result.stream().findFirst();
                    if (record.isPresent()) {
                        return record.get().component1();
                    } else {
                        return 0L;
                    }
                }
            ));
            var nextId = tpToFileMap.values().stream().min(Long::compareTo);
            nextId.map(id -> nextFileIdToFetch = Math.max(nextFileIdToFetch, id));
            LOGGER.debug("Updated next file to fetch: {}", nextFileIdToFetch);
            var logString = lastOffsets.entrySet().stream()
                .map(entry -> "[" + entry.getKey() + " -> " + entry.getValue() + "]")
                .collect(Collectors.joining(", "));
            LOGGER.debug("Updated last offsets due to leadership change: {}", logString);
        }
    }

    private void markWalFilesToDelete() {
        if (remoteLogEndOffsets == null) {
            return;
        }
        var filesAsc = readJooqContext.select(FILES.FILE_ID)
            .from(FILES)
            .where(FILES.STATE.ne(FileStateT.deleting))
            .orderBy(FILES.FILE_ID.asc())
            .fetch();
        writeJooqContext.transaction((final Configuration conf) -> {
            filesAsc.stream().filter(fr -> {
                var batchesInFile = readJooqContext.select(BATCHES.asterisk())
                    .from(BATCHES)
                    .where(BATCHES.FILE_ID.eq(fr.get(FILES.FILE_ID)))
                    .fetch();
                return canDeleteAllBatches(batchesInFile);
            }).forEach(fr -> {
                Routines.markFileToDeleteV1(conf, Instant.now(), fr.get(FILES.FILE_ID));
                LOGGER.debug("Marking WAL file with ID {} as deleting", fr.get(FILES.FILE_ID));
            });
        });
    }

    private boolean canDeleteAllBatches(Result<Record> batchesQueryResult) {
        return batchesQueryResult.stream().allMatch(r -> {
            var tip = new TopicIdPartition(r.get(BATCHES.TOPIC_ID), r.get(BATCHES.PARTITION), null);
            var endOffsetForTip = r.get(BATCHES.LAST_OFFSET); // TODO: off by 1?
            return remoteLogEndOffsets != null && remoteLogEndOffsets.getOrDefault(tip, -1L) >= endOffsetForTip;
        });
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
        var bros = new HashSet<>(readJooqContext.select(BATCHES.asterisk())
            .from(BATCHES)
            .where(BATCHES.FILE_ID.eq(fileRow.get(FILES.FILE_ID)))
            .fetch());
        return bros.stream()
            .map(bro -> {
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
            })
            .filter(bi -> {
                var lastOffset = lastOffsets.getOrDefault(bi.metadata().topicIdPartition(), 0L);
                return bi.metadata().baseOffset() >= lastOffset;
            })
            .collect(Collectors.toSet());
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

    private TreeMap<TopicIdPartition, TreeMap<BatchInfo, MemoryRecords>> byPartition(Map<BatchInfo, MemoryRecords> batchMap) {
        return batchMap
            .entrySet()
            .stream()
            .collect(Collectors.groupingBy(
                e -> e.getKey().metadata().topicIdPartition(),
                () -> new TreeMap<>(topicIdPartitionComparator),
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

    @Override
    public void setRemoteLogEndOffsets(Map<TopicIdPartition, Long> lastOffsets) {
        String partitions = lastOffsets.entrySet().stream()
            .map(entry -> {
                return entry.getKey() + "->" + entry.getValue();
            }).collect(Collectors.joining(", "));
        LOGGER.trace("setRemoteLogEndOffsets: {}", partitions);
        this.remoteLogEndOffsets = lastOffsets.entrySet().stream()
            .collect(Collectors.toMap(
                entry -> new TopicIdPartition(entry.getKey().topicId(), entry.getKey().partition(), null),
                Map.Entry::getValue
            ));
    }

    @Override
    public void setLastOffsets(Map<TopicIdPartition, Long> lastOffsets) {
        String partitions = lastOffsets.entrySet().stream()
            .map(entry -> {
                return entry.getKey() + "->" + entry.getValue();
            }).collect(Collectors.joining(", "));
        LOGGER.trace("setLastOffsets: {}", partitions);
        this.lastOffsets = lastOffsets.entrySet().stream()
            .collect(Collectors.toMap(
                entry -> new TopicIdPartition(entry.getKey().topicId(), entry.getKey().partition(), null),
                Map.Entry::getValue
            ));
        initialized = false;
        initialize();
    }
}
