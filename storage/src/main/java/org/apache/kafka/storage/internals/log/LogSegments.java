/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.storage.internals.log;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.utils.Utils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.function.Predicate;

/**
 * This class encapsulates a thread-safe navigable map of LogSegment instances and provides the
 * required read and write behavior on the map.
 */
public class LogSegments implements Closeable {

    private final TopicPartition topicPartition;
    /* the segments of the log with key being LogSegment base offset and value being a LogSegment */
    private final ConcurrentNavigableMap<Long, LogSegment> segments = new ConcurrentSkipListMap<>();

    /**
     * Create new instance.
     *
     * @param topicPartition the TopicPartition associated with the segments
     *                        (useful for logging purposes)
     */
    public LogSegments(TopicPartition topicPartition) {
        this.topicPartition = topicPartition;
    }

    /**
     * Return true if the segments are empty, false otherwise.
     *
     * This method is thread-safe.
     */
    public boolean isEmpty() {
        return segments.isEmpty();
    }

    /**
     * Return true if the segments are non-empty, false otherwise.
     *
     * This method is thread-safe.
     */
    public boolean nonEmpty() {
        return !isEmpty();
    }

    /**
     * Add the given segment, or replace an existing entry.
     *
     * This method is thread-safe.
     *
     * @param segment the segment to add
     */
    public LogSegment add(LogSegment segment) {
        return this.segments.put(segment.baseOffset(), segment);
    }

    /**
     * Remove the segment at the provided offset.
     *
     * This method is thread-safe.
     *
     * @param offset the offset to be removed
     */
    public void remove(long offset) {
        segments.remove(offset);
    }

    /**
     * Clears all entries.
     *
     * This method is thread-safe.
     */
    public void clear() {
        segments.clear();
    }

    /**
     * Close all segments.
     */
    @Override
    public void close() throws IOException {
        Utils.closeAll(values().toArray(new LogSegment[0]));
    }

    /**
     * Close the handlers for all segments.
     */
    public void closeHandlers() {
        for (LogSegment s : values())
            s.closeHandlers();
    }

    /**
     * Update the directory reference for the log and indices of all segments.
     *
     * @param dir the renamed directory
     */
    public void updateParentDir(File dir) {
        for (LogSegment s : values())
            s.updateParentDir(dir);
    }

    /**
     * This method is thread-safe.
     *
     * @return The number of segments.
     *
     */
    public int numberOfSegments() {
        return segments.size();
    }

    /**
     * @return the base offsets of all segments
     */
    public Collection<Long> baseOffsets() {
        return values().stream().map(LogSegment::baseOffset).toList();
    }

    /**
     * Return true if a segment exists at the provided offset, false otherwise.
     *
     * This method is thread-safe.
     *
     * @param offset the segment to be checked
     */
    public boolean contains(long offset) {
        return segments.containsKey(offset);
    }

    /**
     * Retrieves a segment at the specified offset.
     *
     * This method is thread-safe.
     *
     * @param offset the segment to be retrieved
     *
     * @return the segment if it exists, otherwise Empty.
     */
    public Optional<LogSegment> get(long offset) {
        return Optional.ofNullable(segments.get(offset));
    }

    /**
     * @return an iterator to the log segments ordered from oldest to newest.
     */
    public Collection<LogSegment> values() {
        return segments.values();
    }

    /**
     * @return An iterator to all segments beginning with the segment that includes "from" and ending
     *         with the segment that includes up to "to-1" or the end of the log (if to > end of log).
     */
    public Collection<LogSegment> values(long from, long to) {
        if (from == to) {
            // Handle non-segment-aligned empty sets
            return List.of();
        } else if (to < from) {
            throw new IllegalArgumentException("Invalid log segment range: requested segments in " + topicPartition +
                    " from offset " + from + " which is greater than limit offset " + to);
        } else {
            Long floor = segments.floorKey(from);
            if (floor != null)
                return segments.subMap(floor, to).values();
            return segments.headMap(to).values();
        }
    }

    public Collection<LogSegment> nonActiveLogSegmentsFrom(long from) {
        LogSegment activeSegment = lastSegment().get();
        if (from > activeSegment.baseOffset())
            return List.of();
        else
            return values(from, activeSegment.baseOffset());
    }

    /**
     * Return the entry associated with the greatest offset less than or equal to the given offset,
     * if it exists.
     *
     * This method is thread-safe.
     */
    private Optional<Map.Entry<Long, LogSegment>> floorEntry(long offset) {
        return Optional.ofNullable(segments.floorEntry(offset));
    }

    /**
     * Return the log segment with the greatest offset less than or equal to the given offset,
     * if it exists.
     *
     * This method is thread-safe.
     */
    public Optional<LogSegment> floorSegment(long offset) {
        return floorEntry(offset).map(Map.Entry::getValue);
    }

    /**
     * Return the entry associated with the greatest offset strictly less than the given offset,
     * if it exists.
     *
     * This method is thread-safe.
     */
    private Optional<Map.Entry<Long, LogSegment>> lowerEntry(long offset) {
        return Optional.ofNullable(segments.lowerEntry(offset));
    }

    /**
     * Return the log segment with the greatest offset strictly less than the given offset,
     * if it exists.
     *
     * This method is thread-safe.
     */
    public Optional<LogSegment> lowerSegment(long offset) {
        return lowerEntry(offset).map(Map.Entry::getValue);
    }

    /**
     * Return the entry associated with the smallest offset strictly greater than the given offset,
     * if it exists.
     *
     * This method is thread-safe.
     */
    public Optional<Map.Entry<Long, LogSegment>> higherEntry(long offset) {
        return Optional.ofNullable(segments.higherEntry(offset));
    }

    /**
     * Return the log segment with the smallest offset strictly greater than the given offset,
     * if it exists.
     *
     * This method is thread-safe.
     */
    public Optional<LogSegment> higherSegment(long offset) {
        return higherEntry(offset).map(Map.Entry::getValue);
    }

    /**
     * Return the entry associated with the smallest offset, if it exists.
     *
     * This method is thread-safe.
     */
    public Optional<Map.Entry<Long, LogSegment>> firstEntry() {
        return Optional.ofNullable(segments.firstEntry());
    }

    /**
     * Return the log segment associated with the smallest offset, if it exists.
     *
     * This method is thread-safe.
     */
    public Optional<LogSegment> firstSegment() {
        return firstEntry().map(Map.Entry::getValue);
    }

    /**
     * @return the base offset of the log segment associated with the smallest offset, if it exists
     */
    public OptionalLong firstSegmentBaseOffset() {
        return firstSegment().map(logSegment -> OptionalLong.of(logSegment.baseOffset()))
                .orElseGet(OptionalLong::empty);
    }

    /**
     * Return the entry associated with the greatest offset, if it exists.
     *
     * This method is thread-safe.
     */
    public Optional<Map.Entry<Long, LogSegment>> lastEntry() {
        return Optional.ofNullable(segments.lastEntry());
    }

    /**
     * Return the log segment with the greatest offset, if it exists.
     *
     * This method is thread-safe.
     */
    public Optional<LogSegment> lastSegment() {
        return lastEntry().map(Map.Entry::getValue);
    }

    /**
     * @return an iterable with log segments ordered from lowest base offset to highest,
     *         each segment returned has a base offset strictly greater than the provided baseOffset.
     */
    public Collection<LogSegment> higherSegments(long baseOffset) {
        Long higherOffset = segments.higherKey(baseOffset);
        if (higherOffset != null)
            return segments.tailMap(higherOffset, true).values();
        return List.of();
    }

    /**
     * The active segment that is currently taking appends
     */
    public LogSegment activeSegment() {
        return lastSegment().get();
    }

    public long sizeInBytes() {
        return LogSegments.sizeInBytes(values());
    }

    /**
     * Returns an Iterable containing segments matching the provided predicate.
     *
     * @param predicate the predicate to be used for filtering segments.
     */
    public Collection<LogSegment> filter(Predicate<LogSegment> predicate) {
        return values().stream().filter(predicate).toList();
    }

    /**
     * Calculate a log's size (in bytes) from the provided log segments.
     *
     * @param segments The log segments to calculate the size of
     * @return Sum of the log segments' sizes (in bytes)
     */
    public static long sizeInBytes(Collection<LogSegment> segments) {
        return segments.stream().mapToLong(LogSegment::size).sum();
    }
}
