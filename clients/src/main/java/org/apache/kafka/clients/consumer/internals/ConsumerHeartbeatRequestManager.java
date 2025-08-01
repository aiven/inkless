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
package org.apache.kafka.clients.consumer.internals;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.SubscriptionPattern;
import org.apache.kafka.clients.consumer.internals.events.BackgroundEventHandler;
import org.apache.kafka.clients.consumer.internals.metrics.HeartbeatMetricsManager;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.message.ConsumerGroupHeartbeatRequestData;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.protocol.Errors;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest;
import org.apache.kafka.common.requests.ConsumerGroupHeartbeatResponse;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.common.utils.Timer;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.CloseOptions.GroupMembershipOperation.REMAIN_IN_GROUP;
import static org.apache.kafka.common.requests.ConsumerGroupHeartbeatRequest.REGEX_RESOLUTION_NOT_SUPPORTED_MSG;

/**
 * This is the heartbeat request manager for consumer groups.
 *
 * <p>See {@link AbstractHeartbeatRequestManager} for more details.</p>
 */
public class ConsumerHeartbeatRequestManager extends AbstractHeartbeatRequestManager<ConsumerGroupHeartbeatResponse> {

    /**
     * Membership manager for consumer groups
     */
    private final ConsumerMembershipManager membershipManager;

    /**
     * HeartbeatState manages building the heartbeat requests correctly
     */
    private final HeartbeatState heartbeatState;

    public ConsumerHeartbeatRequestManager(
            final LogContext logContext,
            final Time time,
            final ConsumerConfig config,
            final CoordinatorRequestManager coordinatorRequestManager,
            final SubscriptionState subscriptions,
            final ConsumerMembershipManager membershipManager,
            final BackgroundEventHandler backgroundEventHandler,
            final Metrics metrics) {
        super(logContext, time, config, coordinatorRequestManager, backgroundEventHandler,
            new HeartbeatMetricsManager(metrics));
        this.membershipManager = membershipManager;
        this.heartbeatState = new HeartbeatState(subscriptions, membershipManager, maxPollIntervalMs);
    }

    // Visible for testing
    ConsumerHeartbeatRequestManager(
            final LogContext logContext,
            final Timer timer,
            final ConsumerConfig config,
            final CoordinatorRequestManager coordinatorRequestManager,
            final ConsumerMembershipManager membershipManager,
            final HeartbeatState heartbeatState,
            final HeartbeatRequestState heartbeatRequestState,
            final BackgroundEventHandler backgroundEventHandler,
            final Metrics metrics) {
        super(logContext, timer, config, coordinatorRequestManager, heartbeatRequestState, backgroundEventHandler,
            new HeartbeatMetricsManager(metrics));
        this.membershipManager = membershipManager;
        this.heartbeatState = heartbeatState;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean handleSpecificFailure(Throwable exception) {
        boolean errorHandled = false;
        String errorMessage = exception.getMessage();
        if (exception instanceof UnsupportedVersionException) {
            String message = CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG;
            if (errorMessage.equals(REGEX_RESOLUTION_NOT_SUPPORTED_MSG)) {
                message = REGEX_RESOLUTION_NOT_SUPPORTED_MSG;
                logger.error("{} regex resolution not supported: {}", heartbeatRequestName(), message);
            } else {
                logger.error("{} failed due to unsupported version while sending request: {}", heartbeatRequestName(), errorMessage);
            }
            handleFatalFailure(new UnsupportedVersionException(message, exception));
            errorHandled = true;
        }
        return errorHandled;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean handleSpecificExceptionInResponse(final ConsumerGroupHeartbeatResponse response, final long currentTimeMs) {
        Errors error = errorForResponse(response);
        String errorMessage = errorMessageForResponse(response);
        boolean errorHandled;

        switch (error) {
            // Broker responded with HB not supported, meaning the new protocol is not enabled, so propagate
            // custom message for it. Note that the case where the protocol is not supported at all should fail
            // on the client side when building the request and checking supporting APIs (handled on onFailure).
            case UNSUPPORTED_VERSION:
                logger.error("{} failed due to unsupported version response on broker side: {}",
                    heartbeatRequestName(), CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG);
                handleFatalFailure(error.exception(CONSUMER_PROTOCOL_NOT_SUPPORTED_MSG));
                errorHandled = true;
                break;

            case UNRELEASED_INSTANCE_ID:
                logger.error("{} failed due to unreleased instance id {}: {}",
                    heartbeatRequestName(), membershipManager.groupInstanceId().orElse("null"), errorMessage);
                handleFatalFailure(error.exception(errorMessage));
                errorHandled = true;
                break;

            case FENCED_INSTANCE_ID:
                logger.error("{} failed due to fenced instance id {}: {}. " +
                        "This is expected in the case that the member was removed from the group " +
                        "by an admin client, and another member joined using the same group instance id.",
                    heartbeatRequestName(), membershipManager.groupInstanceId().orElse("null"), errorMessage);
                handleFatalFailure(error.exception(errorMessage));
                errorHandled = true;
                break;

            default:
                errorHandled = false;
        }
        return errorHandled;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void resetHeartbeatState() {
        heartbeatState.reset();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public NetworkClientDelegate.UnsentRequest buildHeartbeatRequest() {
        return new NetworkClientDelegate.UnsentRequest(
            new ConsumerGroupHeartbeatRequest.Builder(this.heartbeatState.buildRequestData()),
            coordinatorRequestManager.coordinator());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String heartbeatRequestName() {
        return "ConsumerGroupHeartbeatRequest";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Errors errorForResponse(ConsumerGroupHeartbeatResponse response) {
        return Errors.forCode(response.data().errorCode());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String errorMessageForResponse(ConsumerGroupHeartbeatResponse response) {
        return response.data().errorMessage();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public long heartbeatIntervalForResponse(ConsumerGroupHeartbeatResponse response) {
        return response.data().heartbeatIntervalMs();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ConsumerMembershipManager membershipManager() {
        return membershipManager;
    }

    @Override
    protected boolean shouldSendLeaveHeartbeatNow() {
        // If the consumer has dynamic membership,
        // we should skip the leaving heartbeat when leaveGroupOperation is REMAIN_IN_GROUP
        if (membershipManager.groupInstanceId().isEmpty() && REMAIN_IN_GROUP == membershipManager.leaveGroupOperation())
            return false;
        return membershipManager().state() == MemberState.LEAVING;
    }

    /**
     * Builds the heartbeat requests correctly, ensuring that all information is sent according to
     * the protocol, but subsequent requests do not send information which has not changed. This
     * is important to ensure that reconciliation completes successfully.
     */
    static class HeartbeatState {
        private final SubscriptionState subscriptions;
        private final ConsumerMembershipManager membershipManager;
        private final int rebalanceTimeoutMs;
        private final SentFields sentFields;

        public HeartbeatState(
                final SubscriptionState subscriptions,
                final ConsumerMembershipManager membershipManager,
                final int rebalanceTimeoutMs) {
            this.subscriptions = subscriptions;
            this.membershipManager = membershipManager;
            this.rebalanceTimeoutMs = rebalanceTimeoutMs;
            this.sentFields = new SentFields();
        }


        public void reset() {
            sentFields.reset();
        }

        @SuppressWarnings("NPathComplexity")
        public ConsumerGroupHeartbeatRequestData buildRequestData() {
            ConsumerGroupHeartbeatRequestData data = new ConsumerGroupHeartbeatRequestData();

            // GroupId - always sent
            data.setGroupId(membershipManager.groupId());

            // MemberId - always sent, it will be generated at Consumer startup.
            data.setMemberId(membershipManager.memberId());

            // MemberEpoch - always sent
            data.setMemberEpoch(membershipManager.memberEpoch());

            // InstanceId - set if present
            membershipManager.groupInstanceId().ifPresent(data::setInstanceId);

            boolean sendAllFields = membershipManager.state() == MemberState.JOINING;

            // RebalanceTimeoutMs - only sent when joining or if it has changed since the last heartbeat
            if (sendAllFields || sentFields.rebalanceTimeoutMs != rebalanceTimeoutMs) {
                data.setRebalanceTimeoutMs(rebalanceTimeoutMs);
                sentFields.rebalanceTimeoutMs = rebalanceTimeoutMs;
            }

            // SubscribedTopicNames - only sent if it has changed since the last heartbeat
            TreeSet<String> subscribedTopicNames = new TreeSet<>(this.subscriptions.subscription());
            if (sendAllFields || !subscribedTopicNames.equals(sentFields.subscribedTopicNames)) {
                data.setSubscribedTopicNames(new ArrayList<>(this.subscriptions.subscription()));
                sentFields.subscribedTopicNames = subscribedTopicNames;
            }

            // SubscribedTopicRegex - only sent if it has changed since the last heartbeat.
            // Send empty string to indicate that a subscribed pattern needs to be removed.
            SubscriptionPattern pattern = subscriptions.subscriptionPattern();
            boolean patternUpdated = !Objects.equals(pattern, sentFields.pattern);
            if ((sendAllFields && pattern != null) || patternUpdated) {
                data.setSubscribedTopicRegex((pattern != null) ? pattern.pattern() : "");
                sentFields.pattern = pattern;
            }

            // ServerAssignor - sent when joining or if it has changed since the last heartbeat
            this.membershipManager.serverAssignor().ifPresent(serverAssignor -> {
                if (sendAllFields || !serverAssignor.equals(sentFields.serverAssignor)) {
                    data.setServerAssignor(serverAssignor);
                    sentFields.serverAssignor = serverAssignor;
                }
            });

            // TopicPartitions - sent when joining or with the first heartbeat after a new assignment from
            // the server was reconciled. This is ensured by resending the topic partitions whenever the
            // local assignment, including its local epoch is changed (although the local epoch is not sent
            // in the heartbeat).
            AbstractMembershipManager.LocalAssignment local = membershipManager.currentAssignment();
            if (sendAllFields || !local.equals(sentFields.localAssignment)) {
                List<ConsumerGroupHeartbeatRequestData.TopicPartitions> topicPartitions =
                        buildTopicPartitionsList(local.partitions);
                data.setTopicPartitions(topicPartitions);
                sentFields.localAssignment = local;
            }

            // RackId - sent when joining
            String rackId = membershipManager.rackId().orElse(null);
            if (sendAllFields) {
                data.setRackId(rackId);
            }

            return data;
        }

        private List<ConsumerGroupHeartbeatRequestData.TopicPartitions> buildTopicPartitionsList(Map<Uuid, SortedSet<Integer>> topicIdPartitions) {
            return topicIdPartitions.entrySet().stream().map(
                    entry -> new ConsumerGroupHeartbeatRequestData.TopicPartitions()
                        .setTopicId(entry.getKey())
                        .setPartitions(new ArrayList<>(entry.getValue())))
                .collect(Collectors.toList());
        }

        // Fields of ConsumerHeartbeatRequest sent in the most recent request
        static class SentFields {
            private int rebalanceTimeoutMs = -1;
            private TreeSet<String> subscribedTopicNames = null;
            private SubscriptionPattern pattern = null;
            private String serverAssignor = null;
            private AbstractMembershipManager.LocalAssignment localAssignment = null;

            SentFields() {}

            void reset() {
                subscribedTopicNames = null;
                rebalanceTimeoutMs = -1;
                serverAssignor = null;
                localAssignment = null;
                pattern = null;
            }
        }
    }
}
