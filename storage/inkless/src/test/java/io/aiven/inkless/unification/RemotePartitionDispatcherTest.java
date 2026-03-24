package io.aiven.inkless.unification;

import org.apache.kafka.common.TopicIdPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.server.common.OffsetAndEpoch;
import org.apache.kafka.server.log.remote.TopicPartitionLog;
import org.apache.kafka.server.log.remote.storage.RemoteLogManager;
import org.apache.kafka.storage.internals.log.UnifiedLog;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RemotePartitionDispatcherTest {

    private static final long TEST_LATCH_TIMEOUT_SECONDS = 60;

    @Test
    public void testApplyLeadersWithNoExistingPartitions() throws InterruptedException {
        var mockRlm = mock(RemoteLogManager.class);
        var mockCph = mock(ConsolidationPoolHandler.class);
        var mockUpdateTask = mock(RemoteLogManager.DisklessConsolidationRemoteOffsetUpdateTask.class);
        when(mockRlm.getRemoteOffsetUpdateTasks(any(), any())).thenReturn(Set.of(mockUpdateTask));

        var newTpl = mock(TopicPartitionLog.class);
        var newTip = new TopicIdPartition(Uuid.randomUuid(), 0, "test-topic");
        var newOffsetAndEpoch = new OffsetAndEpoch(10, 0);
        var newMockLog = mock(UnifiedLog.class);
        when(newTpl.topicPartition()).thenReturn(newTip.topicPartition());
        when(newTpl.unifiedLog()).thenReturn(Optional.of(newMockLog));
        // no current partitions list in the consolidation pool
        when(mockCph.currentPartitions()).thenReturn(Set.of());

        when(mockUpdateTask.topicIdPartition()).thenReturn(newTip);
        when(mockUpdateTask.offsetAndEpoch()).thenReturn(newOffsetAndEpoch);

        ArgumentCaptor<TopicIdPartition> tipCaptor = ArgumentCaptor.forClass(TopicIdPartition.class);
        ArgumentCaptor<OffsetAndEpoch> oaeCaptor = ArgumentCaptor.forClass(OffsetAndEpoch.class);

        // addOffsetsToPartitions runs in a different thread, so we need to coordinate a bit
        CountDownLatch addOffsetsToPartitionDone = new CountDownLatch(1);
        doAnswer(invocation -> {
            addOffsetsToPartitionDone.countDown();
            return null;
        }).when(mockCph).addOffsetsToPartition(any(), any());

        RemotePartitionDispatcher remotePartitionDispatcher = new RemotePartitionDispatcher(mockRlm, mockCph);
        // add one new partition to the consolidation pool
        remotePartitionDispatcher.applyNewLeaders(Set.of(newTpl), Map.of(newTip.topic(), newTip.topicId()));
        // we expect that no partitions will be removed and only one will be added
        verify(mockCph, times(0)).removePartition(any());
        // we expect that the new one will be added
        assertTrue(addOffsetsToPartitionDone.await(TEST_LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS), "addOffsetsToPartition was not called in time");
        verify(mockCph, times(1)).addOffsetsToPartition(tipCaptor.capture(), oaeCaptor.capture());

        assertEquals(newTip, tipCaptor.getValue());
        assertEquals(newOffsetAndEpoch, oaeCaptor.getValue());
    }

    @Test
    public void testApplyLeadersWithExistingPartitions() throws InterruptedException {
        var mockRlm = mock(RemoteLogManager.class);
        var mockCph = mock(ConsolidationPoolHandler.class);
        var mockUpdateTask = mock(RemoteLogManager.DisklessConsolidationRemoteOffsetUpdateTask.class);
        when(mockRlm.getRemoteOffsetUpdateTasks(any(), any())).thenReturn(Set.of(mockUpdateTask));

        var topicId = Uuid.randomUuid();

        var existingTpl = mock(TopicPartitionLog.class);
        var existingTip = new TopicIdPartition(topicId, 1, "test-topic");
        var existingMockLog = mock(UnifiedLog.class);
        when(existingTpl.topicPartition()).thenReturn(existingTip.topicPartition());
        when(existingTpl.unifiedLog()).thenReturn(Optional.of(existingMockLog));
        // current partitions contain test-topic-1
        when(mockCph.currentPartitions()).thenReturn(Set.of(existingTip));

        var newTpl = mock(TopicPartitionLog.class);
        var newTip = new TopicIdPartition(topicId, 0, "test-topic");
        var newOffsetAndEpoch = new OffsetAndEpoch(10, 0);
        var newMockLog = mock(UnifiedLog.class);
        when(newTpl.topicPartition()).thenReturn(newTip.topicPartition());
        when(newTpl.unifiedLog()).thenReturn(Optional.of(newMockLog));

        when(mockUpdateTask.topicIdPartition()).thenReturn(newTip);
        when(mockUpdateTask.offsetAndEpoch()).thenReturn(newOffsetAndEpoch);

        ArgumentCaptor<TopicIdPartition> newTipCaptor = ArgumentCaptor.forClass(TopicIdPartition.class);
        ArgumentCaptor<OffsetAndEpoch> newOaeCaptor = ArgumentCaptor.forClass(OffsetAndEpoch.class);

        // addOffsetsToPartitions runs in a different thread, so we need to coordinate a bit
        CountDownLatch addOffsetsToPartitionDone = new CountDownLatch(1);
        doAnswer(invocation -> {
            addOffsetsToPartitionDone.countDown();
            return null;
        }).when(mockCph).addOffsetsToPartition(any(), any());

        RemotePartitionDispatcher remotePartitionDispatcher = new RemotePartitionDispatcher(mockRlm, mockCph);
        // new partitions are (test-topic-0, test-topic-1) vs current partitions (test-topic-1)
        remotePartitionDispatcher.applyNewLeaders(Set.of(newTpl, existingTpl), Map.of(newTip.topic(), newTip.topicId()));
        // we expect that no partitions will be removed and one will be added (test-topic-0)
        verify(mockCph, times(0)).removePartition(any());
        // we expect that the new one will be added
        assertTrue(addOffsetsToPartitionDone.await(TEST_LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS), "addOffsetsToPartition was not called in time");
        verify(mockCph, times(1)).addOffsetsToPartition(newTipCaptor.capture(), newOaeCaptor.capture());
        verify(mockCph).currentPartitions();

        assertEquals(newTip, newTipCaptor.getValue());
        assertEquals(newOffsetAndEpoch, newOaeCaptor.getValue());
    }

    @Test
    public void testApplyLeadersWithReplacingPartitions() throws InterruptedException {
        var mockRlm = mock(RemoteLogManager.class);
        var mockCph = mock(ConsolidationPoolHandler.class);
        var mockUpdateTask = mock(RemoteLogManager.DisklessConsolidationRemoteOffsetUpdateTask.class);
        when(mockRlm.getRemoteOffsetUpdateTasks(any(), any())).thenReturn(Set.of(mockUpdateTask));

        var topicId = Uuid.randomUuid();

        var existingTpl = mock(TopicPartitionLog.class);
        var existingTip = new TopicIdPartition(topicId, 1, "test-topic");
        var existingMockLog = mock(UnifiedLog.class);
        when(existingTpl.topicPartition()).thenReturn(existingTip.topicPartition());
        when(existingTpl.unifiedLog()).thenReturn(Optional.of(existingMockLog));
        // the pool contains test-topic-1 currently
        when(mockCph.currentPartitions()).thenReturn(Set.of(existingTip));

        var newTpl = mock(TopicPartitionLog.class);
        var newTip = new TopicIdPartition(topicId, 0, "test-topic");
        var newOffsetAndEpoch = new OffsetAndEpoch(10, 0);
        var newMockLog = mock(UnifiedLog.class);
        when(newTpl.topicPartition()).thenReturn(newTip.topicPartition());
        when(newTpl.unifiedLog()).thenReturn(Optional.of(newMockLog));

        when(mockUpdateTask.topicIdPartition()).thenReturn(newTip);
        when(mockUpdateTask.offsetAndEpoch()).thenReturn(newOffsetAndEpoch);

        ArgumentCaptor<TopicIdPartition> newTipCaptor = ArgumentCaptor.forClass(TopicIdPartition.class);
        ArgumentCaptor<OffsetAndEpoch> newOaeCaptor = ArgumentCaptor.forClass(OffsetAndEpoch.class);
        ArgumentCaptor<TopicIdPartition> existingTipCaptor = ArgumentCaptor.forClass(TopicIdPartition.class);

        // addOffsetsToPartitions runs in a different thread, so we need to coordinate a bit
        CountDownLatch addOffsetsToPartitionDone = new CountDownLatch(1);
        doAnswer(invocation -> {
            addOffsetsToPartitionDone.countDown();
            return null;
        }).when(mockCph).addOffsetsToPartition(any(), any());

        RemotePartitionDispatcher remotePartitionDispatcher = new RemotePartitionDispatcher(mockRlm, mockCph);
        // we replace test-topic-1 with test-topic-0
        remotePartitionDispatcher.applyNewLeaders(Set.of(newTpl), Map.of(newTip.topic(), newTip.topicId()));
        // we expect that the existing partition will be removed
        verify(mockCph, times(1)).removePartition(existingTipCaptor.capture());
        // we also expect that the new one will be added
        assertTrue(addOffsetsToPartitionDone.await(TEST_LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS), "addOffsetsToPartition was not called in time");
        verify(mockCph, times(1)).addOffsetsToPartition(newTipCaptor.capture(), newOaeCaptor.capture());

        assertEquals(newTip, newTipCaptor.getValue());
        assertEquals(newOffsetAndEpoch, newOaeCaptor.getValue());
        assertEquals(existingTip, existingTipCaptor.getValue());
    }

    @Test
    public void testApplyFollowersWithNoExistingPartitions() throws InterruptedException {
        var mockRlm = mock(RemoteLogManager.class);
        var mockCph = mock(ConsolidationPoolHandler.class);
        var mockUpdateTask = mock(RemoteLogManager.DisklessConsolidationRemoteOffsetUpdateTask.class);
        when(mockRlm.getRemoteOffsetUpdateTasks(any(), any())).thenReturn(Set.of(mockUpdateTask));

        var newTpl = mock(TopicPartitionLog.class);
        var newTip = new TopicIdPartition(Uuid.randomUuid(), 0, "test-topic");
        var newOffsetAndEpoch = new OffsetAndEpoch(10, 0);
        var newMockLog = mock(UnifiedLog.class);
        when(newTpl.topicPartition()).thenReturn(newTip.topicPartition());
        when(newTpl.unifiedLog()).thenReturn(Optional.of(newMockLog));
        // no current partitions list in the consolidation pool
        when(mockCph.currentPartitions()).thenReturn(Set.of());

        when(mockUpdateTask.topicIdPartition()).thenReturn(newTip);
        when(mockUpdateTask.offsetAndEpoch()).thenReturn(newOffsetAndEpoch);

        ArgumentCaptor<TopicIdPartition> tipCaptor = ArgumentCaptor.forClass(TopicIdPartition.class);
        ArgumentCaptor<OffsetAndEpoch> oaeCaptor = ArgumentCaptor.forClass(OffsetAndEpoch.class);

        // addOffsetsToPartitions runs in a different thread, so we need to coordinate a bit
        CountDownLatch addOffsetsToPartitionDone = new CountDownLatch(1);
        doAnswer(invocation -> {
            addOffsetsToPartitionDone.countDown();
            return null;
        }).when(mockCph).addOffsetsToPartition(any(), any());

        RemotePartitionDispatcher remotePartitionDispatcher = new RemotePartitionDispatcher(mockRlm, mockCph);
        // add one new partition to the consolidation pool
        remotePartitionDispatcher.applyNewFollowers(Set.of(newTpl), Map.of(newTip.topic(), newTip.topicId()));
        // we expect that no partitions will be removed and only one will be added
        verify(mockCph, times(0)).removePartition(any());
        // we also expect that the new one will be added
        assertTrue(addOffsetsToPartitionDone.await(TEST_LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS), "addOffsetsToPartition was not called in time");
        verify(mockCph, times(1)).addOffsetsToPartition(tipCaptor.capture(), oaeCaptor.capture());

        assertEquals(newTip, tipCaptor.getValue());
        assertEquals(newOffsetAndEpoch, oaeCaptor.getValue());
    }

    @Test
    public void testApplyFollowersWithExistingPartitions() throws InterruptedException {
        var mockRlm = mock(RemoteLogManager.class);
        var mockCph = mock(ConsolidationPoolHandler.class);
        var mockUpdateTask = mock(RemoteLogManager.DisklessConsolidationRemoteOffsetUpdateTask.class);
        when(mockRlm.getRemoteOffsetUpdateTasks(any(), any())).thenReturn(Set.of(mockUpdateTask));

        var topicId = Uuid.randomUuid();

        var existingTpl = mock(TopicPartitionLog.class);
        var existingTip = new TopicIdPartition(topicId, 1, "test-topic");
        var existingMockLog = mock(UnifiedLog.class);
        when(existingTpl.topicPartition()).thenReturn(existingTip.topicPartition());
        when(existingTpl.unifiedLog()).thenReturn(Optional.of(existingMockLog));
        // current partitions contain test-topic-1
        when(mockCph.currentPartitions()).thenReturn(Set.of(existingTip));

        var newTpl = mock(TopicPartitionLog.class);
        var newTip = new TopicIdPartition(topicId, 0, "test-topic");
        var newOffsetAndEpoch = new OffsetAndEpoch(10, 0);
        var newMockLog = mock(UnifiedLog.class);
        when(newTpl.topicPartition()).thenReturn(newTip.topicPartition());
        when(newTpl.unifiedLog()).thenReturn(Optional.of(newMockLog));

        when(mockUpdateTask.topicIdPartition()).thenReturn(newTip);
        when(mockUpdateTask.offsetAndEpoch()).thenReturn(newOffsetAndEpoch);

        ArgumentCaptor<TopicIdPartition> newTipCaptor = ArgumentCaptor.forClass(TopicIdPartition.class);
        ArgumentCaptor<OffsetAndEpoch> newOaeCaptor = ArgumentCaptor.forClass(OffsetAndEpoch.class);

        // addOffsetsToPartitions runs in a different thread, so we need to coordinate a bit
        CountDownLatch addOffsetsToPartitionDone = new CountDownLatch(1);
        doAnswer(invocation -> {
            addOffsetsToPartitionDone.countDown();
            return null;
        }).when(mockCph).addOffsetsToPartition(any(), any());

        RemotePartitionDispatcher remotePartitionDispatcher = new RemotePartitionDispatcher(mockRlm, mockCph);
        // new partitions are (test-topic-0, test-topic-1) vs current partitions (test-topic-1)
        remotePartitionDispatcher.applyNewFollowers(Set.of(newTpl, existingTpl), Map.of(newTip.topic(), newTip.topicId()));
        // we expect that no partitions will be removed and one will be added (test-topic-0)
        verify(mockCph, times(0)).removePartition(any());
        // we also expect that the new one will be added
        assertTrue(addOffsetsToPartitionDone.await(TEST_LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS), "addOffsetsToPartition was not called in time");
        verify(mockCph, times(1)).addOffsetsToPartition(newTipCaptor.capture(), newOaeCaptor.capture());
        verify(mockCph).currentPartitions();

        assertEquals(newTip, newTipCaptor.getValue());
        assertEquals(newOffsetAndEpoch, newOaeCaptor.getValue());
    }

    @Test
    public void testApplyFollowersWithReplacingPartitions() throws InterruptedException {
        var mockRlm = mock(RemoteLogManager.class);
        var mockCph = mock(ConsolidationPoolHandler.class);
        var mockUpdateTask = mock(RemoteLogManager.DisklessConsolidationRemoteOffsetUpdateTask.class);
        when(mockRlm.getRemoteOffsetUpdateTasks(any(), any())).thenReturn(Set.of(mockUpdateTask));

        var topicId = Uuid.randomUuid();

        var existingTpl = mock(TopicPartitionLog.class);
        var existingTip = new TopicIdPartition(topicId, 1, "test-topic");
        var existingMockLog = mock(UnifiedLog.class);
        when(existingTpl.topicPartition()).thenReturn(existingTip.topicPartition());
        when(existingTpl.unifiedLog()).thenReturn(Optional.of(existingMockLog));
        // the pool contains test-topic-1 currently
        when(mockCph.currentPartitions()).thenReturn(Set.of(existingTip));

        var newTpl = mock(TopicPartitionLog.class);
        var newTip = new TopicIdPartition(topicId, 0, "test-topic");
        var newOffsetAndEpoch = new OffsetAndEpoch(10, 0);
        var newMockLog = mock(UnifiedLog.class);
        when(newTpl.topicPartition()).thenReturn(newTip.topicPartition());
        when(newTpl.unifiedLog()).thenReturn(Optional.of(newMockLog));

        when(mockUpdateTask.topicIdPartition()).thenReturn(newTip);
        when(mockUpdateTask.offsetAndEpoch()).thenReturn(newOffsetAndEpoch);

        ArgumentCaptor<TopicIdPartition> newTipCaptor = ArgumentCaptor.forClass(TopicIdPartition.class);
        ArgumentCaptor<OffsetAndEpoch> newOaeCaptor = ArgumentCaptor.forClass(OffsetAndEpoch.class);
        ArgumentCaptor<TopicIdPartition> existingTipCaptor = ArgumentCaptor.forClass(TopicIdPartition.class);

        // addOffsetsToPartitions runs in a different thread, so we need to coordinate a bit
        CountDownLatch addOffsetsToPartitionDone = new CountDownLatch(1);
        doAnswer(invocation -> {
            addOffsetsToPartitionDone.countDown();
            return null;
        }).when(mockCph).addOffsetsToPartition(any(), any());

        RemotePartitionDispatcher remotePartitionDispatcher = new RemotePartitionDispatcher(mockRlm, mockCph);
        // we replace test-topic-1 with test-topic-0
        remotePartitionDispatcher.applyNewFollowers(Set.of(newTpl), Map.of(newTip.topic(), newTip.topicId()));
        // we expect that the existing partition will be removed
        verify(mockCph, times(1)).removePartition(existingTipCaptor.capture());
        // we also expect that the new one will be added
        assertTrue(addOffsetsToPartitionDone.await(TEST_LATCH_TIMEOUT_SECONDS, TimeUnit.SECONDS), "addOffsetsToPartition was not called in time");
        verify(mockCph, times(1)).addOffsetsToPartition(newTipCaptor.capture(), newOaeCaptor.capture());

        assertEquals(newTip, newTipCaptor.getValue());
        assertEquals(newOffsetAndEpoch, newOaeCaptor.getValue());
        assertEquals(existingTip, existingTipCaptor.getValue());
    }

    @Test
    public void testApplyDeletedPartitions() {
        var mockRlm = mock(RemoteLogManager.class);
        var mockCph = mock(ConsolidationPoolHandler.class);
        var mockUpdateTask = mock(RemoteLogManager.DisklessConsolidationRemoteOffsetUpdateTask.class);
        when(mockRlm.getRemoteOffsetUpdateTasks(any(), any())).thenReturn(Set.of(mockUpdateTask));

        var topicId = Uuid.randomUuid();

        var existingTpl = mock(TopicPartitionLog.class);
        var existingTip = new TopicIdPartition(topicId, 1, "test-topic");
        var existingMockLog = mock(UnifiedLog.class);
        when(existingTpl.topicPartition()).thenReturn(existingTip.topicPartition());
        when(existingTpl.unifiedLog()).thenReturn(Optional.of(existingMockLog));
        // the pool contains test-topic-1 currently
        when(mockCph.currentPartitions()).thenReturn(Set.of(existingTip));

        ArgumentCaptor<TopicIdPartition> existingTipCaptor = ArgumentCaptor.forClass(TopicIdPartition.class);

        RemotePartitionDispatcher remotePartitionDispatcher = new RemotePartitionDispatcher(mockRlm, mockCph);
        // we replace test-topic-1 with test-topic-0
        remotePartitionDispatcher.applyPartitionDeletes(Set.of(existingTip.topicPartition()), Map.of(existingTip.topic(), existingTip.topicId()));
        // we expect that the existing partition will be removed
        verify(mockCph, times(1)).removePartition(existingTipCaptor.capture());

        assertEquals(existingTip, existingTipCaptor.getValue());
    }
}
