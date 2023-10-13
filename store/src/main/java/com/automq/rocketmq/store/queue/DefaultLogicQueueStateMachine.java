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

package com.automq.rocketmq.store.queue;

import com.automq.rocketmq.store.MessageStoreImpl;
import com.automq.rocketmq.store.api.MessageStateMachine;
import com.automq.rocketmq.store.exception.StoreErrorCode;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.model.generated.CheckPoint;
import com.automq.rocketmq.store.model.kv.BatchDeleteRequest;
import com.automq.rocketmq.store.model.kv.BatchRequest;
import com.automq.rocketmq.store.model.kv.BatchWriteRequest;
import com.automq.rocketmq.store.model.metadata.ConsumerGroupMetadata;
import com.automq.rocketmq.store.model.operation.AckOperation;
import com.automq.rocketmq.store.model.operation.ChangeInvisibleDurationOperation;
import com.automq.rocketmq.store.model.operation.OperationSnapshot;
import com.automq.rocketmq.store.model.operation.PopOperation;
import com.automq.rocketmq.store.service.api.KVService;
import com.automq.rocketmq.store.util.SerializeUtil;
import com.automq.stream.utils.FutureUtil;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.roaringbitmap.RoaringBitmap;
import org.roaringbitmap.buffer.ImmutableRoaringBitmap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.automq.rocketmq.store.MessageStoreImpl.KV_NAMESPACE_CHECK_POINT;
import static com.automq.rocketmq.store.MessageStoreImpl.KV_NAMESPACE_FIFO_INDEX;
import static com.automq.rocketmq.store.MessageStoreImpl.KV_NAMESPACE_TIMER_TAG;
import static com.automq.rocketmq.store.util.SerializeUtil.buildCheckPointKey;
import static com.automq.rocketmq.store.util.SerializeUtil.buildCheckPointValue;
import static com.automq.rocketmq.store.util.SerializeUtil.buildOrderIndexKey;
import static com.automq.rocketmq.store.util.SerializeUtil.buildOrderIndexValue;
import static com.automq.rocketmq.store.util.SerializeUtil.buildReceiptHandle;
import static com.automq.rocketmq.store.util.SerializeUtil.buildTimerTagKey;

public class DefaultLogicQueueStateMachine implements MessageStateMachine {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultLogicQueueStateMachine.class);
    private final long topicId;
    private final int queueId;
    private Map<Long/*consumerGroup*/, ConsumerGroupMetadata> consumerGroupMetadataMap;
    private final Map<Long/*consumerGroup*/, AckCommitter> ackCommitterMap = new HashMap<>();
    private final Map<Long/*consumerGroup*/, AckCommitter> retryAckCommitterMap = new HashMap<>();
    private long currentOperationOffset = -1;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();
    private final Lock reentrantLock = lock.readLock();
    private final Lock exclusiveLock = lock.writeLock();
    private final KVService kvService;
    private final String identity;

    public DefaultLogicQueueStateMachine(long topicId, int queueId, KVService kvService) {
        this.consumerGroupMetadataMap = new ConcurrentHashMap<>();
        this.kvService = kvService;
        this.topicId = topicId;
        this.queueId = queueId;
        this.identity = "[DefaultStateMachine-" + topicId + "-" + queueId + "]";
    }

    @Override
    public long topicId() {
        return topicId;
    }

    @Override
    public int queueId() {
        return queueId;
    }

    @Override
    public CompletableFuture<Void> replayPopOperation(long operationOffset, PopOperation operation) {
        reentrantLock.lock();
        try {
            this.currentOperationOffset = operationOffset;
            switch (operation.popOperationType()) {
                case POP_NORMAL:
                    return replayPopNormalOperation(operationOffset, operation);
                case POP_ORDER:
                    return replayPopFifoOperation(operationOffset, operation);
                case POP_RETRY:
                    return replayPopRetryOperation(operationOffset, operation);
                default:
                    throw new StoreException(StoreErrorCode.ILLEGAL_ARGUMENT, "Unknown pop operation type");
            }
        } catch (Exception e) {
            Throwable cause = FutureUtil.cause(e);
            LOGGER.error("{}: Replay pop operation failed", identity, cause);
            return CompletableFuture.failedFuture(cause);
        } finally {
            reentrantLock.unlock();
        }
    }

    private CompletableFuture<Void> replayPopNormalOperation(long operationOffset,
        PopOperation operation) throws StoreException {
        long topicId = operation.topicId();
        int queueId = operation.queueId();
        long offset = operation.offset();
        long consumerGroupId = operation.consumerGroupId();
        long operationId = operationOffset;
        long operationTimestamp = operation.operationTimestamp();
        long nextVisibleTimestamp = operation.operationTimestamp() + operation.invisibleDuration();
        int count = operation.count();

        LOGGER.trace("Replay pop operation: topicId={}, queueId={}, offset={}, consumerGroupId={}, operationId={}, operationTimestamp={}, nextVisibleTimestamp={}",
            topicId, queueId, offset, consumerGroupId, operationId, operationTimestamp, nextVisibleTimestamp);

        // update consume offset, data or retry stream
        ConsumerGroupMetadata metadata = this.consumerGroupMetadataMap.computeIfAbsent(consumerGroupId, k -> new ConsumerGroupMetadata(consumerGroupId));
        if (metadata.getConsumeOffset() < offset + 1) {
            metadata.setConsumeOffset(offset + 1);
        }
        if (operation.isEndMark()) {
            // if this is a pop-last operation, it only needs to update consume offset and advance ack offset
            long baseOffset = offset - count + 1;
            for (int i = 0; i < count; i++) {
                long currOffset = baseOffset + i;
                this.getAckCommitter(consumerGroupId).commitAck(currOffset);
            }
            return CompletableFuture.completedFuture(null);
        }

        List<BatchRequest> requestList = new ArrayList<>();
        // write a ck for this offset
        BatchWriteRequest writeCheckPointRequest = new BatchWriteRequest(KV_NAMESPACE_CHECK_POINT,
            buildCheckPointKey(topicId, queueId, operationId),
            buildCheckPointValue(topicId, queueId, offset,
                count,
                consumerGroupId, operationId, operation.popOperationType(), operationTimestamp, nextVisibleTimestamp));
        requestList.add(writeCheckPointRequest);

        BatchWriteRequest writeTimerTagRequest = new BatchWriteRequest(KV_NAMESPACE_TIMER_TAG,
            buildTimerTagKey(nextVisibleTimestamp, topicId, queueId, operationId),
            buildReceiptHandle(consumerGroupId, topicId, queueId, operationId));
        requestList.add(writeTimerTagRequest);

        kvService.batch(requestList.toArray(new BatchRequest[0]));
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> replayPopRetryOperation(long operationOffset,
        PopOperation operation) throws StoreException {
        long topicId = operation.topicId();
        int queueId = operation.queueId();
        long offset = operation.offset();
        long consumerGroupId = operation.consumerGroupId();
        long operationId = operationOffset;
        long operationTimestamp = operation.operationTimestamp();
        long nextVisibleTimestamp = operation.operationTimestamp() + operation.invisibleDuration();
        int count = operation.count();

        LOGGER.trace("Replay pop retry operation: topicId={}, queueId={}, offset={}, consumerGroupId={}, operationId={}, operationTimestamp={}, nextVisibleTimestamp={}",
            topicId, queueId, offset, consumerGroupId, operationId, operationTimestamp, nextVisibleTimestamp);

        // update consume offset, data or retry stream
        ConsumerGroupMetadata metadata = this.consumerGroupMetadataMap.computeIfAbsent(consumerGroupId, k -> new ConsumerGroupMetadata(consumerGroupId));
        if (metadata.getRetryConsumeOffset() < offset + 1) {
            metadata.setRetryConsumeOffset(offset + 1);
        }
        if (operation.isEndMark()) {
            // if this is a pop-last operation, it only needs to update consume offset and advance ack offset
            long baseOffset = offset - count + 1;
            for (int i = 0; i < count; i++) {
                long currOffset = baseOffset + i;
                this.getAckCommitter(consumerGroupId).commitAck(currOffset);
            }
            return CompletableFuture.completedFuture(null);
        }

        List<BatchRequest> requestList = new ArrayList<>();
        // write a ck for this offset
        BatchWriteRequest writeCheckPointRequest = new BatchWriteRequest(KV_NAMESPACE_CHECK_POINT,
            buildCheckPointKey(topicId, queueId, operationId),
            buildCheckPointValue(topicId, queueId, offset,
                count,
                consumerGroupId, operationId, operation.popOperationType(), operationTimestamp, nextVisibleTimestamp));
        requestList.add(writeCheckPointRequest);

        BatchWriteRequest writeTimerTagRequest = new BatchWriteRequest(KV_NAMESPACE_TIMER_TAG,
            buildTimerTagKey(nextVisibleTimestamp, topicId, queueId, operationId),
            buildReceiptHandle(consumerGroupId, topicId, queueId, operationId));
        requestList.add(writeTimerTagRequest);

        kvService.batch(requestList.toArray(new BatchRequest[0]));
        return CompletableFuture.completedFuture(null);
    }

    private CompletableFuture<Void> replayPopFifoOperation(long operationOffset,
        PopOperation operation) throws StoreException {
        long topicId = operation.topicId();
        int queueId = operation.queueId();
        long offset = operation.offset();
        long consumerGroupId = operation.consumerGroupId();
        long operationId = operationOffset;
        long operationTimestamp = operation.operationTimestamp();
        long nextVisibleTimestamp = operation.operationTimestamp() + operation.invisibleDuration();

        int count = operation.count();

        LOGGER.trace("Replay pop fifo operation: topicId={}, queueId={}, offset={}, consumerGroupId={}, operationId={}, operationTimestamp={}, nextVisibleTimestamp={}",
            topicId, queueId, offset, consumerGroupId, operationId, operationTimestamp, nextVisibleTimestamp);

        // update consume offset
        ConsumerGroupMetadata metadata = this.consumerGroupMetadataMap.computeIfAbsent(consumerGroupId, k -> new ConsumerGroupMetadata(consumerGroupId));
        if (metadata.getConsumeOffset() < offset + 1) {
            metadata.setConsumeOffset(offset + 1);
        }
        if (operation.isEndMark()) {
            // if this is a pop-last operation, it only needs to update consume offset and advance ack offset
            long baseOffset = offset - count + 1;
            for (int i = 0; i < count; i++) {
                long currOffset = baseOffset + i;
                this.getAckCommitter(consumerGroupId).commitAck(currOffset);
            }
            return CompletableFuture.completedFuture(null);
        }

        List<BatchRequest> requestList = new ArrayList<>();
        // write a ck for this offset
        BatchWriteRequest writeCheckPointRequest = new BatchWriteRequest(KV_NAMESPACE_CHECK_POINT,
            buildCheckPointKey(topicId, queueId, operationId),
            buildCheckPointValue(topicId, queueId, offset,
                count,
                consumerGroupId, operationId, operation.popOperationType(), operationTimestamp, nextVisibleTimestamp));
        requestList.add(writeCheckPointRequest);

        BatchWriteRequest writeTimerTagRequest = new BatchWriteRequest(KV_NAMESPACE_TIMER_TAG,
            buildTimerTagKey(nextVisibleTimestamp, topicId, queueId, operationId),
            buildReceiptHandle(consumerGroupId, topicId, queueId, operationId));
        requestList.add(writeTimerTagRequest);

        // if this message is orderly, write order index for each offset in this operation to KV service
        long baseOffset = offset - count + 1;
        for (int i = 0; i < count; i++) {
            long currOffset = baseOffset + i;
            BatchWriteRequest writeOrderIndexRequest = new BatchWriteRequest(KV_NAMESPACE_FIFO_INDEX,
                buildOrderIndexKey(consumerGroupId, topicId, queueId, currOffset), buildOrderIndexValue(operationId));
            requestList.add(writeOrderIndexRequest);
        }

        kvService.batch(requestList.toArray(new BatchRequest[0]));
        return CompletableFuture.completedFuture(null);
    }

    private AckCommitter getAckCommitter(long consumerGroupId) {
        return getAckCommitter(consumerGroupId, new RoaringBitmap());
    }

    private AckCommitter getAckCommitter(long consumerGroupId, RoaringBitmap bitmap) {
        ConsumerGroupMetadata metadata = this.consumerGroupMetadataMap.computeIfAbsent(consumerGroupId, k -> new ConsumerGroupMetadata(consumerGroupId));
        return this.ackCommitterMap.computeIfAbsent(consumerGroupId, k -> new AckCommitter(metadata.getAckOffset(), metadata::setAckOffset, bitmap));
    }

    private AckCommitter getRetryAckCommitter(long consumerGroupId) {
        return getRetryAckCommitter(consumerGroupId, new RoaringBitmap());
    }

    private AckCommitter getRetryAckCommitter(long consumerGroupId, RoaringBitmap bitmap) {
        ConsumerGroupMetadata metadata = this.consumerGroupMetadataMap.computeIfAbsent(consumerGroupId, k -> new ConsumerGroupMetadata(consumerGroupId));
        return this.retryAckCommitterMap.computeIfAbsent(consumerGroupId, k -> new AckCommitter(metadata.getRetryAckOffset(), metadata::setRetryAckOffset, bitmap));
    }

    @Override
    public CompletableFuture<Void> replayAckOperation(long operationOffset, AckOperation operation) {
        long topicId = operation.topicId();
        int queueId = operation.queueId();
        long operationId = operation.operationId();
        AckOperation.AckOperationType type = operation.ackOperationType();

        LOGGER.trace("Replay ack operation: topicId={}, queueId={}, operationId={}, type={}",
            topicId, queueId, operationId, type);

        reentrantLock.lock();
        try {
            currentOperationOffset = operationOffset;
            // check if ck exists
            byte[] ckKey = buildCheckPointKey(topicId, queueId, operationId);
            byte[] ckValue = kvService.get(KV_NAMESPACE_CHECK_POINT, ckKey);
            if (ckValue == null) {
                throw new StoreException(StoreErrorCode.ILLEGAL_ARGUMENT, "Ack operation failed, check point not found");
            }
            CheckPoint ck = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(ckValue));
            List<BatchRequest> requestList = new ArrayList<>();
            int count = ck.count();

            // delete ck, timer tag
            BatchDeleteRequest deleteCheckPointRequest = new BatchDeleteRequest(KV_NAMESPACE_CHECK_POINT, ckKey);
            requestList.add(deleteCheckPointRequest);

            BatchDeleteRequest deleteTimerTagRequest = new BatchDeleteRequest(KV_NAMESPACE_TIMER_TAG,
                buildTimerTagKey(ck.nextVisibleTimestamp(), topicId, queueId, operationId));
            requestList.add(deleteTimerTagRequest);
            long consumerGroupId = ck.consumerGroupId();
            long baseOffset = ck.messageOffset() - count + 1;
            for (int i = 0; i < count; i++) {
                long currOffset = baseOffset + i;
                if (ck.popOperationType() == PopOperation.PopOperationType.POP_NORMAL.ordinal() ||
                    (ck.popOperationType() == PopOperation.PopOperationType.POP_ORDER.ordinal() && type == AckOperation.AckOperationType.ACK_NORMAL)) {
                    this.getAckCommitter(consumerGroupId).commitAck(currOffset);
                }
                if (ck.popOperationType() == PopOperation.PopOperationType.POP_RETRY.ordinal()) {
                    this.getRetryAckCommitter(consumerGroupId).commitAck(currOffset);
                }
                // delete order index
                if (ck.popOperationType() == PopOperation.PopOperationType.POP_ORDER.ordinal()) {
                    BatchDeleteRequest deleteOrderIndexRequest = new BatchDeleteRequest(KV_NAMESPACE_FIFO_INDEX,
                        buildOrderIndexKey(ck.consumerGroupId(), topicId, queueId, currOffset));
                    requestList.add(deleteOrderIndexRequest);
                }
            }

            kvService.batch(requestList.toArray(new BatchRequest[0]));
        } catch (StoreException e) {
            LOGGER.error("{}: Replay ack operation failed", identity, e);
            return CompletableFuture.failedFuture(e);
        } finally {
            reentrantLock.unlock();
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> replayChangeInvisibleDurationOperation(long operationOffset,
        ChangeInvisibleDurationOperation operation) {
        long invisibleDuration = operation.invisibleDuration();
        long operationTimestamp = operation.operationTimestamp();
        long topic = operation.topicId();
        int queue = operation.queueId();
        long operationId = operation.operationId();
        long nextInvisibleTimestamp = operationTimestamp + invisibleDuration;

        LOGGER.trace("Replay change invisible duration operation: topicId={}, queueId={}, operationId={}, invisibleDuration={}, operationTimestamp={}, nextInvisibleTimestamp={}",
            topic, queue, operationId, invisibleDuration, operationTimestamp, nextInvisibleTimestamp);

        reentrantLock.lock();
        try {
            currentOperationOffset = operationOffset;
            // Check if check point exists.
            byte[] checkPointKey = buildCheckPointKey(topic, queue, operationId);
            byte[] buffer = kvService.get(KV_NAMESPACE_CHECK_POINT, checkPointKey);
            if (buffer == null) {
                throw new StoreException(StoreErrorCode.ILLEGAL_ARGUMENT, "Change invisible duration operation failed, check point not found");
            }
            // Delete last timer tag.
            CheckPoint checkPoint = CheckPoint.getRootAsCheckPoint(ByteBuffer.wrap(buffer));
            BatchDeleteRequest deleteLastTimerTagRequest = new BatchDeleteRequest(KV_NAMESPACE_TIMER_TAG,
                buildTimerTagKey(checkPoint.nextVisibleTimestamp(), checkPoint.topicId(), checkPoint.queueId(), checkPoint.operationId()));

            // Write new check point and timer tag.
            BatchWriteRequest writeCheckPointRequest = new BatchWriteRequest(KV_NAMESPACE_CHECK_POINT,
                buildCheckPointKey(checkPoint.topicId(), checkPoint.queueId(), checkPoint.operationId()),
                buildCheckPointValue(checkPoint.topicId(), checkPoint.queueId(), checkPoint.messageOffset(), checkPoint.count(),
                    checkPoint.consumerGroupId(), checkPoint.operationId(), PopOperation.PopOperationType.valueOf(checkPoint.popOperationType()),
                    checkPoint.deliveryTimestamp(), nextInvisibleTimestamp));

            BatchWriteRequest writeTimerTagRequest = new BatchWriteRequest(KV_NAMESPACE_TIMER_TAG,
                buildTimerTagKey(nextInvisibleTimestamp, checkPoint.topicId(), checkPoint.queueId(), checkPoint.operationId()),
                buildReceiptHandle(checkPoint.consumerGroupId(), checkPoint.topicId(), checkPoint.queueId(), checkPoint.operationId()));
            kvService.batch(deleteLastTimerTagRequest, writeCheckPointRequest, writeTimerTagRequest);
        } catch (StoreException e) {
            LOGGER.error("{}: Replay change invisible duration operation failed", identity, e);
            return CompletableFuture.failedFuture(e);
        } finally {
            reentrantLock.unlock();
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<OperationSnapshot> takeSnapshot() {
        exclusiveLock.lock();
        try {
            List<OperationSnapshot.ConsumerGroupMetadataSnapshot> metadataSnapshots = consumerGroupMetadataMap.values().stream().map(metadata -> {
                try {
                    return new OperationSnapshot.ConsumerGroupMetadataSnapshot(metadata.getConsumerGroupId(), metadata.getConsumeOffset(), metadata.getAckOffset(),
                        metadata.getRetryConsumeOffset(), metadata.getRetryAckOffset(),
                        getAckCommitter(metadata.getConsumerGroupId()).getAckBitmapBuffer().array(),
                        getRetryAckCommitter(metadata.getConsumerGroupId()).getAckBitmapBuffer().array());
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }).collect(Collectors.toList());
            long snapshotVersion = kvService.takeSnapshot();
            OperationSnapshot snapshot = new OperationSnapshot(currentOperationOffset, snapshotVersion, metadataSnapshots);
            return CompletableFuture.completedFuture(snapshot);
        } catch (Exception e) {
            Throwable cause = FutureUtil.cause(e);
            LOGGER.error("{}: Take snapshot failed", identity, cause);
            return CompletableFuture.failedFuture(e);
        } finally {
            exclusiveLock.unlock();
        }
    }

    @Override
    public CompletableFuture<Void> loadSnapshot(OperationSnapshot snapshot) {
        exclusiveLock.lock();
        try {
            this.consumerGroupMetadataMap = snapshot.getConsumerGroupMetadataList().stream().collect(Collectors.toMap(
                ConsumerGroupMetadata::getConsumerGroupId, metadataSnapshot ->
                    new ConsumerGroupMetadata(metadataSnapshot.getConsumerGroupId(), metadataSnapshot.getConsumeOffset(), metadataSnapshot.getAckOffset(),
                        metadataSnapshot.getRetryConsumeOffset(), metadataSnapshot.getRetryAckOffset())));
            snapshot.getConsumerGroupMetadataList().forEach(metadataSnapshot -> {
                RoaringBitmap bitmap = new RoaringBitmap(new ImmutableRoaringBitmap(ByteBuffer.wrap(metadataSnapshot.getAckOffsetBitmapBuffer())));
                getAckCommitter(metadataSnapshot.getConsumerGroupId(), bitmap);
                RoaringBitmap retryBitmap = new RoaringBitmap(new ImmutableRoaringBitmap(ByteBuffer.wrap(metadataSnapshot.getRetryAckOffsetBitmapBuffer())));
                getRetryAckCommitter(metadataSnapshot.getConsumerGroupId(), retryBitmap);
            });
            this.currentOperationOffset = snapshot.getSnapshotEndOffset();
            // recover states in kv service
            snapshot.getCheckPoints().forEach(checkPoint -> {
                try {
                    List<BatchRequest> requestList = new ArrayList<>();
                    // write ck
                    BatchWriteRequest writeCheckPointRequest = new BatchWriteRequest(KV_NAMESPACE_CHECK_POINT,
                        buildCheckPointKey(checkPoint.topicId(), checkPoint.queueId(), checkPoint.operationId()),
                        buildCheckPointValue(checkPoint.topicId(), checkPoint.queueId(), checkPoint.messageOffset(), checkPoint.count(),
                            checkPoint.consumerGroupId(), checkPoint.operationId(), PopOperation.PopOperationType.valueOf(checkPoint.popOperationType()),
                            checkPoint.deliveryTimestamp(), checkPoint.nextVisibleTimestamp()));
                    requestList.add(writeCheckPointRequest);
                    // write timer tag
                    BatchWriteRequest writeTimerTagRequest = new BatchWriteRequest(KV_NAMESPACE_TIMER_TAG,
                        buildTimerTagKey(checkPoint.nextVisibleTimestamp(), checkPoint.topicId(), checkPoint.queueId(), checkPoint.operationId()),
                        buildReceiptHandle(checkPoint.consumerGroupId(), checkPoint.topicId(), checkPoint.queueId(), checkPoint.operationId()));
                    requestList.add(writeTimerTagRequest);
                    // write order index
                    if (checkPoint.popOperationType() == PopOperation.PopOperationType.POP_ORDER.ordinal()) {
                        long baseOffset = checkPoint.messageOffset() - checkPoint.count() + 1;
                        for (int i = 0; i < checkPoint.count(); i++) {
                            long currOffset = baseOffset + i;
                            BatchWriteRequest writeOrderIndexRequest = new BatchWriteRequest(KV_NAMESPACE_FIFO_INDEX,
                                buildOrderIndexKey(checkPoint.consumerGroupId(), checkPoint.topicId(), checkPoint.queueId(), currOffset), buildOrderIndexValue(checkPoint.operationId()));
                            requestList.add(writeOrderIndexRequest);
                        }
                    }
                    if (!requestList.isEmpty()) {
                        kvService.batch(requestList.toArray(new BatchRequest[0]));
                    }
                } catch (StoreException e) {
                    LOGGER.error("{}: Recover from snapshot: {} failed", identity, snapshot, e);
                    throw new RuntimeException(e);
                }
            });
        } catch (Exception e) {
            Throwable cause = FutureUtil.cause(e);
            LOGGER.error("{}: Load snapshot:{} failed", identity, snapshot, cause);
            return CompletableFuture.failedFuture(e);
        } finally {
            exclusiveLock.unlock();
        }
        return CompletableFuture.completedFuture(null);
    }

    @Override
    public CompletableFuture<Void> clear() {
        exclusiveLock.lock();
        try {
            this.consumerGroupMetadataMap.clear();
            this.ackCommitterMap.clear();
            this.retryAckCommitterMap.clear();
            this.currentOperationOffset = -1;
            List<CheckPoint> checkPointList = new ArrayList<>();
            byte[] tqPrefix = SerializeUtil.buildCheckPointPrefix(topicId, queueId);
            kvService.iterate(MessageStoreImpl.KV_NAMESPACE_CHECK_POINT, tqPrefix, null, null, (key, value) -> {
                CheckPoint checkPoint = SerializeUtil.decodeCheckPoint(ByteBuffer.wrap(value));
                checkPointList.add(checkPoint);
            });
            // clear ck, timer tag, order index
            List<BatchRequest> requestList = new ArrayList<>();
            checkPointList.forEach(checkPoint -> {
                BatchDeleteRequest deleteCheckPointRequest = new BatchDeleteRequest(KV_NAMESPACE_CHECK_POINT,
                    buildCheckPointKey(checkPoint.topicId(), checkPoint.queueId(), checkPoint.operationId()));
                requestList.add(deleteCheckPointRequest);

                BatchDeleteRequest deleteTimerTagRequest = new BatchDeleteRequest(KV_NAMESPACE_TIMER_TAG,
                    buildTimerTagKey(checkPoint.nextVisibleTimestamp(), checkPoint.topicId(), checkPoint.queueId(), checkPoint.operationId()));
                requestList.add(deleteTimerTagRequest);

                if (checkPoint.popOperationType() == PopOperation.PopOperationType.POP_ORDER.ordinal()) {
                    long baseOffset = checkPoint.messageOffset() - checkPoint.count() + 1;
                    for (int i = 0; i < checkPoint.count(); i++) {
                        long currOffset = baseOffset + i;
                        BatchDeleteRequest deleteOrderIndexRequest = new BatchDeleteRequest(KV_NAMESPACE_FIFO_INDEX,
                            buildOrderIndexKey(checkPoint.consumerGroupId(), checkPoint.topicId(), checkPoint.queueId(), currOffset));
                        requestList.add(deleteOrderIndexRequest);
                    }
                }
            });
            if (!requestList.isEmpty()) {
                kvService.batch(requestList.toArray(new BatchRequest[0]));
            }
            return CompletableFuture.completedFuture(null);
        } catch (StoreException e) {
            LOGGER.error("{}: Clear failed", identity, e);
            return CompletableFuture.failedFuture(e);
        } finally {
            exclusiveLock.unlock();
        }
    }

    @Override
    public CompletableFuture<Long> consumeOffset(long consumerGroupId) {
        return CompletableFuture.completedFuture(consumerGroupMetadataMap.computeIfAbsent(consumerGroupId, k -> new ConsumerGroupMetadata(consumerGroupId)).getConsumeOffset());
    }

    @Override
    public CompletableFuture<Long> ackOffset(long consumerGroupId) {
        return CompletableFuture.completedFuture(consumerGroupMetadataMap.computeIfAbsent(consumerGroupId, k -> new ConsumerGroupMetadata(consumerGroupId)).getAckOffset());
    }

    @Override
    public CompletableFuture<Long> retryConsumeOffset(long consumerGroupId) {
        return CompletableFuture.completedFuture(consumerGroupMetadataMap.computeIfAbsent(consumerGroupId, k -> new ConsumerGroupMetadata(consumerGroupId)).getRetryConsumeOffset());
    }

    @Override
    public CompletableFuture<Long> retryAckOffset(long consumerGroupId) {
        return CompletableFuture.completedFuture(consumerGroupMetadataMap.computeIfAbsent(consumerGroupId, k -> new ConsumerGroupMetadata(consumerGroupId)).getRetryAckOffset());
    }

    @Override
    public CompletableFuture<Boolean> isLocked(long consumerGroupId, long offset) {
        exclusiveLock.lock();
        try {
            byte[] lockKey = buildOrderIndexKey(consumerGroupId, topicId, queueId, offset);
            return CompletableFuture.completedFuture(kvService.get(KV_NAMESPACE_FIFO_INDEX, lockKey) != null);
        } catch (StoreException e) {
            return CompletableFuture.failedFuture(e);
        } finally {
            exclusiveLock.unlock();
        }
    }

    static class AckCommitter {
        private long ackOffset;
        private RoaringBitmap bitmap;
        private Consumer<Long> ackAdvanceFn;
        private long baseOffset;

        public AckCommitter(long ackOffset, Consumer<Long> ackAdvanceFn) {
            this(ackOffset, ackAdvanceFn, new RoaringBitmap());
        }

        public AckCommitter(long ackOffset, Consumer<Long> ackAdvanceFn, RoaringBitmap bitmap) {
            this.ackOffset = ackOffset;
            this.ackAdvanceFn = ackAdvanceFn;
            this.bitmap = bitmap;
            this.baseOffset = ackOffset;
        }

        public void commitAck(long offset) {
            if (offset >= ackOffset) {
                // TODO: how to handle overflow?
                int offsetInBitmap = (int) (offset - baseOffset);
                bitmap.add(offsetInBitmap);
                boolean advance = false;
                while (bitmap.contains((int) (ackOffset - baseOffset))) {
                    ackOffset++;
                    advance = true;
                }
                if (advance) {
                    ackAdvanceFn.accept(ackOffset);
                }
            }
        }

        public ByteBuffer getAckBitmapBuffer() throws IOException {
            int length = bitmap.serializedSizeInBytes();
            ByteBuffer buffer = ByteBuffer.allocate(length);
            bitmap.serialize(buffer);
            return buffer;
        }
    }
}