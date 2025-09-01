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
package org.apache.kafka.network;

import org.apache.kafka.common.message.*;
import org.apache.kafka.common.network.ClientInformation;
import org.apache.kafka.common.requests.*;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.BooleanNode;
import com.fasterxml.jackson.databind.node.DoubleNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.LongNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;

import java.util.Optional;

public class RequestConvertToJson {

    public static JsonNode request(AbstractRequest request) {
        switch (request.apiKey()) {
            case ADD_OFFSETS_TO_TXN:
                return AddOffsetsToTxnRequestDataJsonConverter.write(((AddOffsetsToTxnRequest) request).data(), request.version());
            case ADD_PARTITIONS_TO_TXN:
                return AddPartitionsToTxnRequestDataJsonConverter.write(((AddPartitionsToTxnRequest) request).data(), request.version());
            case ADD_RAFT_VOTER:
                return AddRaftVoterRequestDataJsonConverter.write(((AddRaftVoterRequest) request).data(), request.version());
            case ALLOCATE_PRODUCER_IDS:
                return AllocateProducerIdsRequestDataJsonConverter.write(((AllocateProducerIdsRequest) request).data(), request.version());
            case ALTER_CLIENT_QUOTAS:
                return AlterClientQuotasRequestDataJsonConverter.write(((AlterClientQuotasRequest) request).data(), request.version());
            case ALTER_CONFIGS:
                return AlterConfigsRequestDataJsonConverter.write(((AlterConfigsRequest) request).data(), request.version());
            case ALTER_PARTITION_REASSIGNMENTS:
                return AlterPartitionReassignmentsRequestDataJsonConverter.write(((AlterPartitionReassignmentsRequest) request).data(), request.version());
            case ALTER_PARTITION:
                return AlterPartitionRequestDataJsonConverter.write(((AlterPartitionRequest) request).data(), request.version());
            case ALTER_REPLICA_LOG_DIRS:
                return AlterReplicaLogDirsRequestDataJsonConverter.write(((AlterReplicaLogDirsRequest) request).data(), request.version());
            case ALTER_SHARE_GROUP_OFFSETS:
                return AlterShareGroupOffsetsRequestDataJsonConverter.write(((AlterShareGroupOffsetsRequest) request).data(), request.version());
            case ALTER_USER_SCRAM_CREDENTIALS:
                return AlterUserScramCredentialsRequestDataJsonConverter.write(((AlterUserScramCredentialsRequest) request).data(), request.version());
            case API_VERSIONS:
                return ApiVersionsRequestDataJsonConverter.write(((ApiVersionsRequest) request).data(), request.version());
            case ASSIGN_REPLICAS_TO_DIRS:
                return AssignReplicasToDirsRequestDataJsonConverter.write(((AssignReplicasToDirsRequest) request).data(), request.version());
            case BEGIN_QUORUM_EPOCH:
                return BeginQuorumEpochRequestDataJsonConverter.write(((BeginQuorumEpochRequest) request).data(), request.version());
            case BROKER_HEARTBEAT:
                return BrokerHeartbeatRequestDataJsonConverter.write(((BrokerHeartbeatRequest) request).data(), request.version());
            case BROKER_REGISTRATION:
                return BrokerRegistrationRequestDataJsonConverter.write(((BrokerRegistrationRequest) request).data(), request.version());
            case CONSUMER_GROUP_DESCRIBE:
                return ConsumerGroupDescribeRequestDataJsonConverter.write(((ConsumerGroupDescribeRequest) request).data(), request.version());
            case CONSUMER_GROUP_HEARTBEAT:
                return ConsumerGroupHeartbeatRequestDataJsonConverter.write(((ConsumerGroupHeartbeatRequest) request).data(), request.version());
            case CONTROLLER_REGISTRATION:
                return ControllerRegistrationRequestDataJsonConverter.write(((ControllerRegistrationRequest) request).data(), request.version());
            case CREATE_ACLS:
                return CreateAclsRequestDataJsonConverter.write(((CreateAclsRequest) request).data(), request.version());
            case CREATE_DELEGATION_TOKEN:
                return CreateDelegationTokenRequestDataJsonConverter.write(((CreateDelegationTokenRequest) request).data(), request.version());
            case CREATE_PARTITIONS:
                return CreatePartitionsRequestDataJsonConverter.write(((CreatePartitionsRequest) request).data(), request.version());
            case CREATE_TOPICS:
                return CreateTopicsRequestDataJsonConverter.write(((CreateTopicsRequest) request).data(), request.version());
            case DELETE_ACLS:
                return DeleteAclsRequestDataJsonConverter.write(((DeleteAclsRequest) request).data(), request.version());
            case DELETE_GROUPS:
                return DeleteGroupsRequestDataJsonConverter.write(((DeleteGroupsRequest) request).data(), request.version());
            case DELETE_RECORDS:
                return DeleteRecordsRequestDataJsonConverter.write(((DeleteRecordsRequest) request).data(), request.version());
            case DELETE_SHARE_GROUP_OFFSETS:
                return DeleteShareGroupOffsetsRequestDataJsonConverter.write(((DeleteShareGroupOffsetsRequest) request).data(), request.version());
            case DELETE_SHARE_GROUP_STATE:
                return DeleteShareGroupStateRequestDataJsonConverter.write(((DeleteShareGroupStateRequest) request).data(), request.version());
            case DELETE_TOPICS:
                return DeleteTopicsRequestDataJsonConverter.write(((DeleteTopicsRequest) request).data(), request.version());
            case DESCRIBE_ACLS:
                return DescribeAclsRequestDataJsonConverter.write(((DescribeAclsRequest) request).data(), request.version());
            case DESCRIBE_CLIENT_QUOTAS:
                return DescribeClientQuotasRequestDataJsonConverter.write(((DescribeClientQuotasRequest) request).data(), request.version());
            case DESCRIBE_CLUSTER:
                return DescribeClusterRequestDataJsonConverter.write(((DescribeClusterRequest) request).data(), request.version());
            case DESCRIBE_CONFIGS:
                return DescribeConfigsRequestDataJsonConverter.write(((DescribeConfigsRequest) request).data(), request.version());
            case DESCRIBE_DELEGATION_TOKEN:
                return DescribeDelegationTokenRequestDataJsonConverter.write(((DescribeDelegationTokenRequest) request).data(), request.version());
            case DESCRIBE_GROUPS:
                return DescribeGroupsRequestDataJsonConverter.write(((DescribeGroupsRequest) request).data(), request.version());
            case DESCRIBE_LOG_DIRS:
                return DescribeLogDirsRequestDataJsonConverter.write(((DescribeLogDirsRequest) request).data(), request.version());
            case DESCRIBE_PRODUCERS:
                return DescribeProducersRequestDataJsonConverter.write(((DescribeProducersRequest) request).data(), request.version());
            case DESCRIBE_QUORUM:
                return DescribeQuorumRequestDataJsonConverter.write(((DescribeQuorumRequest) request).data(), request.version());
            case DESCRIBE_SHARE_GROUP_OFFSETS:
                return DescribeShareGroupOffsetsRequestDataJsonConverter.write(((DescribeShareGroupOffsetsRequest) request).data(), request.version());
            case DESCRIBE_TOPIC_PARTITIONS:
                return DescribeTopicPartitionsRequestDataJsonConverter.write(((DescribeTopicPartitionsRequest) request).data(), request.version());
            case DESCRIBE_TRANSACTIONS:
                return DescribeTransactionsRequestDataJsonConverter.write(((DescribeTransactionsRequest) request).data(), request.version());
            case DESCRIBE_USER_SCRAM_CREDENTIALS:
                return DescribeUserScramCredentialsRequestDataJsonConverter.write(((DescribeUserScramCredentialsRequest) request).data(), request.version());
            case ELECT_LEADERS:
                return ElectLeadersRequestDataJsonConverter.write(((ElectLeadersRequest) request).data(), request.version());
            case END_QUORUM_EPOCH:
                return EndQuorumEpochRequestDataJsonConverter.write(((EndQuorumEpochRequest) request).data(), request.version());
            case END_TXN:
                return EndTxnRequestDataJsonConverter.write(((EndTxnRequest) request).data(), request.version());
            case ENVELOPE:
                return EnvelopeRequestDataJsonConverter.write(((EnvelopeRequest) request).data(), request.version());
            case EXPIRE_DELEGATION_TOKEN:
                return ExpireDelegationTokenRequestDataJsonConverter.write(((ExpireDelegationTokenRequest) request).data(), request.version());
            case FETCH:
                return FetchRequestDataJsonConverter.write(((FetchRequest) request).data(), request.version());
            case FETCH_SNAPSHOT:
                return FetchSnapshotRequestDataJsonConverter.write(((FetchSnapshotRequest) request).data(), request.version());
            case FIND_COORDINATOR:
                return FindCoordinatorRequestDataJsonConverter.write(((FindCoordinatorRequest) request).data(), request.version());
            case GET_TELEMETRY_SUBSCRIPTIONS:
                return GetTelemetrySubscriptionsRequestDataJsonConverter.write(((GetTelemetrySubscriptionsRequest) request).data(), request.version());
            case HEARTBEAT:
                return HeartbeatRequestDataJsonConverter.write(((HeartbeatRequest) request).data(), request.version());
            case INCREMENTAL_ALTER_CONFIGS:
                return IncrementalAlterConfigsRequestDataJsonConverter.write(((IncrementalAlterConfigsRequest) request).data(), request.version());
            case INITIALIZE_SHARE_GROUP_STATE:
                return InitializeShareGroupStateRequestDataJsonConverter.write(((InitializeShareGroupStateRequest) request).data(), request.version());
            case INIT_PRODUCER_ID:
                return InitProducerIdRequestDataJsonConverter.write(((InitProducerIdRequest) request).data(), request.version());
            case JOIN_GROUP:
                return JoinGroupRequestDataJsonConverter.write(((JoinGroupRequest) request).data(), request.version());
            case LEAVE_GROUP:
                return LeaveGroupRequestDataJsonConverter.write(((LeaveGroupRequest) request).data(), request.version());
            case LIST_CONFIG_RESOURCES:
                return ListConfigResourcesRequestDataJsonConverter.write(((ListConfigResourcesRequest) request).data(), request.version());
            case LIST_GROUPS:
                return ListGroupsRequestDataJsonConverter.write(((ListGroupsRequest) request).data(), request.version());
            case LIST_OFFSETS:
                return ListOffsetsRequestDataJsonConverter.write(((ListOffsetsRequest) request).data(), request.version());
            case LIST_PARTITION_REASSIGNMENTS:
                return ListPartitionReassignmentsRequestDataJsonConverter.write(((ListPartitionReassignmentsRequest) request).data(), request.version());
            case LIST_TRANSACTIONS:
                return ListTransactionsRequestDataJsonConverter.write(((ListTransactionsRequest) request).data(), request.version());
            case METADATA:
                return MetadataRequestDataJsonConverter.write(((MetadataRequest) request).data(), request.version());
            case OFFSET_COMMIT:
                return OffsetCommitRequestDataJsonConverter.write(((OffsetCommitRequest) request).data(), request.version());
            case OFFSET_DELETE:
                return OffsetDeleteRequestDataJsonConverter.write(((OffsetDeleteRequest) request).data(), request.version());
            case OFFSET_FETCH:
                return OffsetFetchRequestDataJsonConverter.write(((OffsetFetchRequest) request).data(), request.version());
            case OFFSET_FOR_LEADER_EPOCH:
                return OffsetForLeaderEpochRequestDataJsonConverter.write(((OffsetsForLeaderEpochRequest) request).data(), request.version());
            case PRODUCE:
                return ProduceRequestDataJsonConverter.write(((ProduceRequest) request).data(), request.version(), false);
            case PUSH_TELEMETRY:
                return PushTelemetryRequestDataJsonConverter.write(((PushTelemetryRequest) request).data(), request.version());
            case READ_SHARE_GROUP_STATE:
                return ReadShareGroupStateRequestDataJsonConverter.write(((ReadShareGroupStateRequest) request).data(), request.version());
            case READ_SHARE_GROUP_STATE_SUMMARY:
                return ReadShareGroupStateSummaryRequestDataJsonConverter.write(((ReadShareGroupStateSummaryRequest) request).data(), request.version());
            case REMOVE_RAFT_VOTER:
                return RemoveRaftVoterRequestDataJsonConverter.write(((RemoveRaftVoterRequest) request).data(), request.version());
            case RENEW_DELEGATION_TOKEN:
                return RenewDelegationTokenRequestDataJsonConverter.write(((RenewDelegationTokenRequest) request).data(), request.version());
            case SASL_AUTHENTICATE:
                return SaslAuthenticateRequestDataJsonConverter.write(((SaslAuthenticateRequest) request).data(), request.version());
            case SASL_HANDSHAKE:
                return SaslHandshakeRequestDataJsonConverter.write(((SaslHandshakeRequest) request).data(), request.version());
            case SHARE_ACKNOWLEDGE:
                return ShareAcknowledgeRequestDataJsonConverter.write(((ShareAcknowledgeRequest) request).data(), request.version());
            case SHARE_FETCH:
                return ShareFetchRequestDataJsonConverter.write(((ShareFetchRequest) request).data(), request.version());
            case SHARE_GROUP_DESCRIBE:
                return ShareGroupDescribeRequestDataJsonConverter.write(((ShareGroupDescribeRequest) request).data(), request.version());
            case SHARE_GROUP_HEARTBEAT:
                return ShareGroupHeartbeatRequestDataJsonConverter.write(((ShareGroupHeartbeatRequest) request).data(), request.version());
            case STREAMS_GROUP_DESCRIBE:
                return StreamsGroupDescribeRequestDataJsonConverter.write(((StreamsGroupDescribeRequest) request).data(), request.version());
            case STREAMS_GROUP_HEARTBEAT:
                return StreamsGroupHeartbeatRequestDataJsonConverter.write(((StreamsGroupHeartbeatRequest) request).data(), request.version());
            case SYNC_GROUP:
                return SyncGroupRequestDataJsonConverter.write(((SyncGroupRequest) request).data(), request.version());
            case TXN_OFFSET_COMMIT:
                return TxnOffsetCommitRequestDataJsonConverter.write(((TxnOffsetCommitRequest) request).data(), request.version());
            case UNREGISTER_BROKER:
                return UnregisterBrokerRequestDataJsonConverter.write(((UnregisterBrokerRequest) request).data(), request.version());
            case UPDATE_FEATURES:
                return UpdateFeaturesRequestDataJsonConverter.write(((UpdateFeaturesRequest) request).data(), request.version());
            case UPDATE_RAFT_VOTER:
                return UpdateRaftVoterRequestDataJsonConverter.write(((UpdateRaftVoterRequest) request).data(), request.version());
            case VOTE:
                return VoteRequestDataJsonConverter.write(((VoteRequest) request).data(), request.version());
            case WRITE_SHARE_GROUP_STATE:
                return WriteShareGroupStateRequestDataJsonConverter.write(((WriteShareGroupStateRequest) request).data(), request.version());
            case WRITE_TXN_MARKERS:
                return WriteTxnMarkersRequestDataJsonConverter.write(((WriteTxnMarkersRequest) request).data(), request.version());
            case GET_REPLICA_LOG_INFO:
                return GetReplicaLogInfoRequestDataJsonConverter.write(((GetReplicaLogInfoRequest) request).data(), request.version());
            case CREATE_CLUSTER_LINK:
                return CreateClusterLinkRequestDataJsonConverter.write(((CreateClusterLinkRequest) request).data(), request.version());
            default:
                throw new IllegalStateException("ApiKey " + request.apiKey() + " is not currently handled in `request`, the " +
                    "code should be updated to do so.");
        }
    }

    public static JsonNode response(AbstractResponse response, short version) {
        switch (response.apiKey()) {
            case ADD_OFFSETS_TO_TXN:
                return AddOffsetsToTxnResponseDataJsonConverter.write(((AddOffsetsToTxnResponse) response).data(), version);
            case ADD_PARTITIONS_TO_TXN:
                return AddPartitionsToTxnResponseDataJsonConverter.write(((AddPartitionsToTxnResponse) response).data(), version);
            case ADD_RAFT_VOTER:
                return AddRaftVoterResponseDataJsonConverter.write(((AddRaftVoterResponse) response).data(), version);
            case ALLOCATE_PRODUCER_IDS:
                return AllocateProducerIdsResponseDataJsonConverter.write(((AllocateProducerIdsResponse) response).data(), version);
            case ALTER_CLIENT_QUOTAS:
                return AlterClientQuotasResponseDataJsonConverter.write(((AlterClientQuotasResponse) response).data(), version);
            case ALTER_CONFIGS:
                return AlterConfigsResponseDataJsonConverter.write(((AlterConfigsResponse) response).data(), version);
            case ALTER_PARTITION_REASSIGNMENTS:
                return AlterPartitionReassignmentsResponseDataJsonConverter.write(((AlterPartitionReassignmentsResponse) response).data(), version);
            case ALTER_PARTITION:
                return AlterPartitionResponseDataJsonConverter.write(((AlterPartitionResponse) response).data(), version);
            case ALTER_REPLICA_LOG_DIRS:
                return AlterReplicaLogDirsResponseDataJsonConverter.write(((AlterReplicaLogDirsResponse) response).data(), version);
            case ALTER_SHARE_GROUP_OFFSETS:
                return AlterShareGroupOffsetsResponseDataJsonConverter.write(((AlterShareGroupOffsetsResponse) response).data(), version);
            case ALTER_USER_SCRAM_CREDENTIALS:
                return AlterUserScramCredentialsResponseDataJsonConverter.write(((AlterUserScramCredentialsResponse) response).data(), version);
            case API_VERSIONS:
                return ApiVersionsResponseDataJsonConverter.write(((ApiVersionsResponse) response).data(), version);
            case ASSIGN_REPLICAS_TO_DIRS:
                return AssignReplicasToDirsResponseDataJsonConverter.write(((AssignReplicasToDirsResponse) response).data(), version);
            case BEGIN_QUORUM_EPOCH:
                return BeginQuorumEpochResponseDataJsonConverter.write(((BeginQuorumEpochResponse) response).data(), version);
            case BROKER_HEARTBEAT:
                return BrokerHeartbeatResponseDataJsonConverter.write(((BrokerHeartbeatResponse) response).data(), version);
            case BROKER_REGISTRATION:
                return BrokerRegistrationResponseDataJsonConverter.write(((BrokerRegistrationResponse) response).data(), version);
            case CONSUMER_GROUP_DESCRIBE:
                return ConsumerGroupDescribeResponseDataJsonConverter.write(((ConsumerGroupDescribeResponse) response).data(), version);
            case CONSUMER_GROUP_HEARTBEAT:
                return ConsumerGroupHeartbeatResponseDataJsonConverter.write(((ConsumerGroupHeartbeatResponse) response).data(), version);
            case CONTROLLER_REGISTRATION:
                return ControllerRegistrationResponseDataJsonConverter.write(((ControllerRegistrationResponse) response).data(), version);
            case CREATE_ACLS:
                return CreateAclsResponseDataJsonConverter.write(((CreateAclsResponse) response).data(), version);
            case CREATE_DELEGATION_TOKEN:
                return CreateDelegationTokenResponseDataJsonConverter.write(((CreateDelegationTokenResponse) response).data(), version);
            case CREATE_PARTITIONS:
                return CreatePartitionsResponseDataJsonConverter.write(((CreatePartitionsResponse) response).data(), version);
            case CREATE_TOPICS:
                return CreateTopicsResponseDataJsonConverter.write(((CreateTopicsResponse) response).data(), version);
            case DELETE_ACLS:
                return DeleteAclsResponseDataJsonConverter.write(((DeleteAclsResponse) response).data(), version);
            case DELETE_GROUPS:
                return DeleteGroupsResponseDataJsonConverter.write(((DeleteGroupsResponse) response).data(), version);
            case DELETE_RECORDS:
                return DeleteRecordsResponseDataJsonConverter.write(((DeleteRecordsResponse) response).data(), version);
            case DELETE_SHARE_GROUP_OFFSETS:
                return DeleteShareGroupOffsetsResponseDataJsonConverter.write(((DeleteShareGroupOffsetsResponse) response).data(), version);
            case DELETE_SHARE_GROUP_STATE:
                return DeleteShareGroupStateResponseDataJsonConverter.write(((DeleteShareGroupStateResponse) response).data(), version);
            case DELETE_TOPICS:
                return DeleteTopicsResponseDataJsonConverter.write(((DeleteTopicsResponse) response).data(), version);
            case DESCRIBE_ACLS:
                return DescribeAclsResponseDataJsonConverter.write(((DescribeAclsResponse) response).data(), version);
            case DESCRIBE_CLIENT_QUOTAS:
                return DescribeClientQuotasResponseDataJsonConverter.write(((DescribeClientQuotasResponse) response).data(), version);
            case DESCRIBE_CLUSTER:
                return DescribeClusterResponseDataJsonConverter.write(((DescribeClusterResponse) response).data(), version);
            case DESCRIBE_CONFIGS:
                return DescribeConfigsResponseDataJsonConverter.write(((DescribeConfigsResponse) response).data(), version);
            case DESCRIBE_DELEGATION_TOKEN:
                return DescribeDelegationTokenResponseDataJsonConverter.write(((DescribeDelegationTokenResponse) response).data(), version);
            case DESCRIBE_GROUPS:
                return DescribeGroupsResponseDataJsonConverter.write(((DescribeGroupsResponse) response).data(), version);
            case DESCRIBE_LOG_DIRS:
                return DescribeLogDirsResponseDataJsonConverter.write(((DescribeLogDirsResponse) response).data(), version);
            case DESCRIBE_PRODUCERS:
                return DescribeProducersResponseDataJsonConverter.write(((DescribeProducersResponse) response).data(), version);
            case DESCRIBE_QUORUM:
                return DescribeQuorumResponseDataJsonConverter.write(((DescribeQuorumResponse) response).data(), version);
            case DESCRIBE_SHARE_GROUP_OFFSETS:
                return DescribeShareGroupOffsetsResponseDataJsonConverter.write(((DescribeShareGroupOffsetsResponse) response).data(), version);
            case DESCRIBE_TOPIC_PARTITIONS:
                return DescribeTopicPartitionsResponseDataJsonConverter.write(((DescribeTopicPartitionsResponse) response).data(), version);
            case DESCRIBE_TRANSACTIONS:
                return DescribeTransactionsResponseDataJsonConverter.write(((DescribeTransactionsResponse) response).data(), version);
            case DESCRIBE_USER_SCRAM_CREDENTIALS:
                return DescribeUserScramCredentialsResponseDataJsonConverter.write(((DescribeUserScramCredentialsResponse) response).data(), version);
            case ELECT_LEADERS:
                return ElectLeadersResponseDataJsonConverter.write(((ElectLeadersResponse) response).data(), version);
            case END_QUORUM_EPOCH:
                return EndQuorumEpochResponseDataJsonConverter.write(((EndQuorumEpochResponse) response).data(), version);
            case END_TXN:
                return EndTxnResponseDataJsonConverter.write(((EndTxnResponse) response).data(), version);
            case ENVELOPE:
                return EnvelopeResponseDataJsonConverter.write(((EnvelopeResponse) response).data(), version);
            case EXPIRE_DELEGATION_TOKEN:
                return ExpireDelegationTokenResponseDataJsonConverter.write(((ExpireDelegationTokenResponse) response).data(), version);
            case FETCH:
                return FetchResponseDataJsonConverter.write(((FetchResponse) response).data(), version, false);
            case FETCH_SNAPSHOT:
                return FetchSnapshotResponseDataJsonConverter.write(((FetchSnapshotResponse) response).data(), version);
            case FIND_COORDINATOR:
                return FindCoordinatorResponseDataJsonConverter.write(((FindCoordinatorResponse) response).data(), version);
            case GET_TELEMETRY_SUBSCRIPTIONS:
                return GetTelemetrySubscriptionsResponseDataJsonConverter.write(((GetTelemetrySubscriptionsResponse) response).data(), version);
            case HEARTBEAT:
                return HeartbeatResponseDataJsonConverter.write(((HeartbeatResponse) response).data(), version);
            case INCREMENTAL_ALTER_CONFIGS:
                return IncrementalAlterConfigsResponseDataJsonConverter.write(((IncrementalAlterConfigsResponse) response).data(), version);
            case INITIALIZE_SHARE_GROUP_STATE:
                return InitializeShareGroupStateResponseDataJsonConverter.write(((InitializeShareGroupStateResponse) response).data(), version);
            case INIT_PRODUCER_ID:
                return InitProducerIdResponseDataJsonConverter.write(((InitProducerIdResponse) response).data(), version);
            case JOIN_GROUP:
                return JoinGroupResponseDataJsonConverter.write(((JoinGroupResponse) response).data(), version);
            case LEAVE_GROUP:
                return LeaveGroupResponseDataJsonConverter.write(((LeaveGroupResponse) response).data(), version);
            case LIST_CONFIG_RESOURCES:
                return ListConfigResourcesResponseDataJsonConverter.write(((ListConfigResourcesResponse) response).data(), version);
            case LIST_GROUPS:
                return ListGroupsResponseDataJsonConverter.write(((ListGroupsResponse) response).data(), version);
            case LIST_OFFSETS:
                return ListOffsetsResponseDataJsonConverter.write(((ListOffsetsResponse) response).data(), version);
            case LIST_PARTITION_REASSIGNMENTS:
                return ListPartitionReassignmentsResponseDataJsonConverter.write(((ListPartitionReassignmentsResponse) response).data(), version);
            case LIST_TRANSACTIONS:
                return ListTransactionsResponseDataJsonConverter.write(((ListTransactionsResponse) response).data(), version);
            case METADATA:
                return MetadataResponseDataJsonConverter.write(((MetadataResponse) response).data(), version);
            case OFFSET_COMMIT:
                return OffsetCommitResponseDataJsonConverter.write(((OffsetCommitResponse) response).data(), version);
            case OFFSET_DELETE:
                return OffsetDeleteResponseDataJsonConverter.write(((OffsetDeleteResponse) response).data(), version);
            case OFFSET_FETCH:
                return OffsetFetchResponseDataJsonConverter.write(((OffsetFetchResponse) response).data(), version);
            case OFFSET_FOR_LEADER_EPOCH:
                return OffsetForLeaderEpochResponseDataJsonConverter.write(((OffsetsForLeaderEpochResponse) response).data(), version);
            case PRODUCE:
                return ProduceResponseDataJsonConverter.write(((ProduceResponse) response).data(), version);
            case PUSH_TELEMETRY:
                return PushTelemetryResponseDataJsonConverter.write(((PushTelemetryResponse) response).data(), version);
            case READ_SHARE_GROUP_STATE:
                return ReadShareGroupStateResponseDataJsonConverter.write(((ReadShareGroupStateResponse) response).data(), version);
            case READ_SHARE_GROUP_STATE_SUMMARY:
                return ReadShareGroupStateSummaryResponseDataJsonConverter.write(((ReadShareGroupStateSummaryResponse) response).data(), version);
            case REMOVE_RAFT_VOTER:
                return RemoveRaftVoterResponseDataJsonConverter.write(((RemoveRaftVoterResponse) response).data(), version);
            case RENEW_DELEGATION_TOKEN:
                return RenewDelegationTokenResponseDataJsonConverter.write(((RenewDelegationTokenResponse) response).data(), version);
            case SASL_AUTHENTICATE:
                return SaslAuthenticateResponseDataJsonConverter.write(((SaslAuthenticateResponse) response).data(), version);
            case SASL_HANDSHAKE:
                return SaslHandshakeResponseDataJsonConverter.write(((SaslHandshakeResponse) response).data(), version);
            case SHARE_ACKNOWLEDGE:
                return ShareAcknowledgeResponseDataJsonConverter.write(((ShareAcknowledgeResponse) response).data(), version);
            case SHARE_FETCH:
                return ShareFetchResponseDataJsonConverter.write(((ShareFetchResponse) response).data(), version);
            case SHARE_GROUP_DESCRIBE:
                return ShareGroupDescribeResponseDataJsonConverter.write(((ShareGroupDescribeResponse) response).data(), version);
            case SHARE_GROUP_HEARTBEAT:
                return ShareGroupHeartbeatResponseDataJsonConverter.write(((ShareGroupHeartbeatResponse) response).data(), version);
            case STREAMS_GROUP_DESCRIBE:
                return StreamsGroupDescribeResponseDataJsonConverter.write(((StreamsGroupDescribeResponse) response).data(), version);
            case STREAMS_GROUP_HEARTBEAT:
                return StreamsGroupHeartbeatResponseDataJsonConverter.write(((StreamsGroupHeartbeatResponse) response).data(), version);
            case SYNC_GROUP:
                return SyncGroupResponseDataJsonConverter.write(((SyncGroupResponse) response).data(), version);
            case TXN_OFFSET_COMMIT:
                return TxnOffsetCommitResponseDataJsonConverter.write(((TxnOffsetCommitResponse) response).data(), version);
            case UNREGISTER_BROKER:
                return UnregisterBrokerResponseDataJsonConverter.write(((UnregisterBrokerResponse) response).data(), version);
            case UPDATE_FEATURES:
                return UpdateFeaturesResponseDataJsonConverter.write(((UpdateFeaturesResponse) response).data(), version);
            case UPDATE_RAFT_VOTER:
                return UpdateRaftVoterResponseDataJsonConverter.write(((UpdateRaftVoterResponse) response).data(), version);
            case VOTE:
                return VoteResponseDataJsonConverter.write(((VoteResponse) response).data(), version);
            case WRITE_SHARE_GROUP_STATE:
                return WriteShareGroupStateResponseDataJsonConverter.write(((WriteShareGroupStateResponse) response).data(), version);
            case WRITE_TXN_MARKERS:
                return WriteTxnMarkersResponseDataJsonConverter.write(((WriteTxnMarkersResponse) response).data(), version);
            case GET_REPLICA_LOG_INFO:
                return GetReplicaLogInfoResponseDataJsonConverter.write(((GetReplicaLogInfoResponse) response).data(), version);
            case CREATE_CLUSTER_LINK:
                return CreateClusterLinkResponseDataJsonConverter.write(((CreateClusterLinkResponse) response).data(), version);
            default:
                throw new IllegalStateException("ApiKey " + response.apiKey() + " is not currently handled in `response`, the " +
                    "code should be updated to do so.");
        }
    }

    public static JsonNode requestHeaderNode(RequestHeader header) {
        ObjectNode node = (ObjectNode) RequestHeaderDataJsonConverter.write(
            header.data(), header.headerVersion(), false
        );
        node.set("requestApiKeyName", new TextNode(header.apiKey().toString()));
        if (header.isApiVersionDeprecated()) {
            node.set("requestApiVersionDeprecated", BooleanNode.TRUE);
        }
        return node;
    }

    public static JsonNode requestDesc(RequestHeader header, Optional<JsonNode> requestNode, boolean isForwarded) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.set("isForwarded", isForwarded ? BooleanNode.TRUE : BooleanNode.FALSE);
        node.set("requestHeader", requestHeaderNode(header));
        node.set("request", requestNode.orElse(new TextNode("")));
        return node;
    }

    public static JsonNode clientInfoNode(ClientInformation clientInfo) {
        ObjectNode node = JsonNodeFactory.instance.objectNode();
        node.set("softwareName", new TextNode(clientInfo.softwareName()));
        node.set("softwareVersion", new TextNode(clientInfo.softwareVersion()));
        return node;
    }

    public static JsonNode requestDescMetrics(RequestHeader header, Optional<JsonNode> requestNode, Optional<JsonNode> responseNode,
                                              RequestContext context, Session session, boolean isForwarded,
                                              double totalTimeMs, double requestQueueTimeMs, double apiLocalTimeMs,
                                              double apiRemoteTimeMs, long apiThrottleTimeMs, double responseQueueTimeMs,
                                              double responseSendTimeMs, long temporaryMemoryBytes,
                                              double messageConversionsTimeMs) {
        ObjectNode node = (ObjectNode) requestDesc(header, requestNode, isForwarded);
        node.set("response", responseNode.orElse(new TextNode("")));
        node.set("connection", new TextNode(context.connectionId));
        node.set("totalTimeMs", new DoubleNode(totalTimeMs));
        node.set("requestQueueTimeMs", new DoubleNode(requestQueueTimeMs));
        node.set("localTimeMs", new DoubleNode(apiLocalTimeMs));
        node.set("remoteTimeMs", new DoubleNode(apiRemoteTimeMs));
        node.set("throttleTimeMs", new LongNode(apiThrottleTimeMs));
        node.set("responseQueueTimeMs", new DoubleNode(responseQueueTimeMs));
        node.set("sendTimeMs", new DoubleNode(responseSendTimeMs));
        node.set("securityProtocol", new TextNode(context.securityProtocol.toString()));
        node.set("principal", new TextNode(session.principal.toString()));
        node.set("listener", new TextNode(context.listenerName.value()));
        node.set("clientInformation", clientInfoNode(context.clientInformation));
        if (temporaryMemoryBytes > 0) {
            node.set("temporaryMemoryBytes", new LongNode(temporaryMemoryBytes));
        }
        if (messageConversionsTimeMs > 0) {
            node.set("messageConversionsTime", new DoubleNode(messageConversionsTimeMs));
        }
        return node;
    }
}
