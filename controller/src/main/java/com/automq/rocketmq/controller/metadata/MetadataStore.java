/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.automq.rocketmq.controller.metadata;

import apache.rocketmq.controller.v1.AssignmentStatus;
import apache.rocketmq.controller.v1.ConsumerGroup;
import apache.rocketmq.controller.v1.GroupType;
import apache.rocketmq.controller.v1.StreamRole;
import apache.rocketmq.controller.v1.Topic;
import apache.rocketmq.controller.v1.S3StreamObject;
import apache.rocketmq.controller.v1.S3WALObject;
import apache.rocketmq.controller.v1.StreamMetadata;
import apache.rocketmq.controller.v1.MessageType;
import com.automq.rocketmq.common.StoreHandle;
import com.automq.rocketmq.common.config.ControllerConfig;
import com.automq.rocketmq.controller.exception.ControllerException;
import com.automq.rocketmq.controller.metadata.database.dao.Lease;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import com.automq.rocketmq.controller.metadata.database.dao.QueueAssignment;
import java.io.Closeable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.ibatis.session.SqlSession;

public interface MetadataStore extends Closeable {

    ControllerConfig config();

    SqlSession openSession();

    ControllerClient controllerClient();

    void addBrokerNode(Node node);

    void setLease(Lease lease);

    void setRole(Role role);

    StoreHandle getStoreHandle();

    void setStoreHandle(StoreHandle storeHandle);

    void start();

    /**
     * Register broker into metadata store and return broker epoch
     *
     * @return broker epoch
     * @throws ControllerException If there is an I/O error.
     */
    CompletableFuture<Node> registerBrokerNode(String name, String address, String instanceId);

    /**
     * Register broker into metadata store and return broker epoch
     *
     * @return broker epoch
     * @throws ControllerException If there is an I/O error.
     */
    void registerCurrentNode(String name, String address, String instanceId) throws ControllerException;

    void keepAlive(int nodeId, long epoch, boolean goingAway);

    CompletableFuture<Long> createTopic(String topicName, int queueNum,
        List<MessageType> acceptMessageTypesList) throws ControllerException;

    CompletableFuture<Void> deleteTopic(long topicId);

    CompletableFuture<Topic> describeTopic(Long topicId, String topicName);

    CompletableFuture<List<Topic>> listTopics();

    CompletableFuture<Topic> updateTopic(long topicId,
        @Nullable String topicName,
        @Nullable Integer queueNumber,
        @Nonnull List<MessageType> acceptMessageTypesList) throws ControllerException;

    /**
     * Check if current controller is playing leader role
     *
     * @return true if leader; false otherwise
     * @throws ControllerException If there is any I/O error
     */
    boolean isLeader() throws ControllerException;

    boolean hasAliveBrokerNodes();

    String leaderAddress() throws ControllerException;

    /**
     * List queue assignments according to criteria.
     *
     * @param topicId   Optional topic-id
     * @param srcNodeId Optional source node-id
     * @param dstNodeId Optional destination node-id
     * @param status    Optional queue assignment status
     * @return List of the queue assignments meeting the specified criteria
     */
    CompletableFuture<List<QueueAssignment>> listAssignments(Long topicId, Integer srcNodeId, Integer dstNodeId,
        AssignmentStatus status);

    CompletableFuture<Void> reassignMessageQueue(long topicId, int queueId, int dstNodeId) throws ControllerException;

    CompletableFuture<Void> markMessageQueueAssignable(long topicId, int queueId);

    CompletableFuture<Void> commitOffset(long groupId, long topicId, int queueId, long offset);

    CompletableFuture<Long> createGroup(String groupName, int maxRetry, GroupType type,
        long dlq) throws ControllerException;

    CompletableFuture<StreamMetadata> getStream(long topicId, int queueId, Long groupId, StreamRole streamRole);

    /**
     * Invoked when store has closed the queue.
     *
     * @param topicId Topic ID
     * @param queueId Queue ID
     */
    CompletableFuture<Void> onQueueClosed(long topicId, int queueId);

    CompletableFuture<ConsumerGroup> describeConsumerGroup(Long groupId, String groupName);

    CompletableFuture<Void> trimStream(long streamId, long streamEpoch, long newStartOffset) throws ControllerException;

    CompletableFuture<StreamMetadata> openStream(long streamId, long streamEpoch, int nodeId);

    CompletableFuture<Void> closeStream(long streamId, long streamEpoch, int nodeId);

    CompletableFuture<List<StreamMetadata>> listOpenStreams(int nodeId);

    CompletableFuture<Long> prepareS3Objects(int count, int ttlInMinutes);

    CompletableFuture<Void> commitWalObject(S3WALObject walObject, List<S3StreamObject> streamObjects,
        List<Long> compactedObjects);

    CompletableFuture<Void> commitStreamObject(S3StreamObject streamObject,
        List<Long> compactedObjects) throws ControllerException;

    CompletableFuture<List<S3WALObject>> listWALObjects();

    CompletableFuture<List<S3WALObject>> listWALObjects(long streamId, long startOffset, long endOffset, int limit);

    CompletableFuture<List<S3StreamObject>> listStreamObjects(long streamId, long startOffset, long endOffset,
        int limit);

    CompletableFuture<Long> getOrCreateRetryStream(String groupName, long topicId,
        int queueId) throws ControllerException;

    CompletableFuture<Long> getConsumerOffset(long consumerGroupId, long topicId, int queueId);

    String addressOfNode(int nodeId);

    CompletableFuture<Pair<List<S3StreamObject>, List<S3WALObject>>> listObjects(long streamId, long startOffset, long endOffset, int limit);

    boolean maintainLeadershipWithSharedLock(SqlSession session);
}
