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

package com.automq.stream.s3;

import com.automq.stream.s3.model.StreamRecordBatch;
import com.automq.stream.s3.objects.CommitStreamSetObjectRequest;
import com.automq.stream.s3.objects.ObjectManager;
import com.automq.stream.s3.objects.ObjectStreamRange;
import com.automq.stream.s3.objects.StreamObject;
import com.automq.stream.s3.operator.S3Operator;
import com.automq.stream.utils.AsyncRateLimiter;
import com.automq.stream.utils.FutureUtil;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.automq.stream.s3.metadata.ObjectUtils.NOOP_OBJECT_ID;

public class DeltaWALUploadTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(DeltaWALUploadTask.class);
    final boolean forceSplit;
    private final Logger s3ObjectLogger;
    private final Map<Long, List<StreamRecordBatch>> streamRecordsMap;
    private final int objectBlockSize;
    private final int objectPartSize;
    private final int streamSplitSizeThreshold;
    private final ObjectManager objectManager;
    private final S3Operator s3Operator;
    private final boolean s3ObjectLogEnable;
    private final CompletableFuture<Long> prepareCf = new CompletableFuture<>();
    private final CompletableFuture<CommitStreamSetObjectRequest> uploadCf = new CompletableFuture<>();
    private final ExecutorService executor;
    private final double rate;
    private final AsyncRateLimiter limiter;
    private long startTimestamp;
    private volatile CommitStreamSetObjectRequest commitStreamSetObjectRequest;

    public DeltaWALUploadTask(Config config, Map<Long, List<StreamRecordBatch>> streamRecordsMap,
        ObjectManager objectManager, S3Operator s3Operator,
        ExecutorService executor, boolean forceSplit, double rate) {
        this.s3ObjectLogger = S3ObjectLogger.logger(String.format("[DeltaWALUploadTask id=%d] ", config.nodeId()));
        this.streamRecordsMap = streamRecordsMap;
        this.objectBlockSize = config.objectBlockSize();
        this.objectPartSize = config.objectPartSize();
        this.streamSplitSizeThreshold = config.streamSplitSize();
        this.s3ObjectLogEnable = config.objectLogEnable();
        this.objectManager = objectManager;
        this.s3Operator = s3Operator;
        this.forceSplit = forceSplit;
        this.executor = executor;
        this.rate = rate;
        this.limiter = new AsyncRateLimiter(rate);
    }

    public static Builder builder() {
        return new Builder();
    }

    public CompletableFuture<Long> prepare() {
        startTimestamp = System.currentTimeMillis();
        if (forceSplit) {
            prepareCf.complete(NOOP_OBJECT_ID);
        } else {
            objectManager
                .prepareObject(1, TimeUnit.MINUTES.toMillis(60))
                .thenAcceptAsync(prepareCf::complete, executor)
                .exceptionally(ex -> {
                    prepareCf.completeExceptionally(ex);
                    return null;
                });
        }
        return prepareCf;
    }

    public CompletableFuture<CommitStreamSetObjectRequest> upload() {
        prepareCf.thenAcceptAsync(objectId -> FutureUtil.exec(() -> upload0(objectId), uploadCf, LOGGER, "upload"), executor);
        return uploadCf;
    }

    private void upload0(long objectId) {
        List<Long> streamIds = new ArrayList<>(streamRecordsMap.keySet());
        Collections.sort(streamIds);
        CommitStreamSetObjectRequest request = new CommitStreamSetObjectRequest();

        ObjectWriter streamSetObject;
        if (forceSplit) {
            // when only has one stream, we only need to write the stream data.
            streamSetObject = ObjectWriter.noop(objectId);
        } else {
            streamSetObject = ObjectWriter.writer(objectId, s3Operator, objectBlockSize, objectPartSize);
        }

        List<CompletableFuture<Void>> streamObjectCfList = new LinkedList<>();

        List<CompletableFuture<Void>> streamSetWriteCfList = new LinkedList<>();
        for (Long streamId : streamIds) {
            List<StreamRecordBatch> streamRecords = streamRecordsMap.get(streamId);
            int streamSize = streamRecords.stream().mapToInt(StreamRecordBatch::size).sum();
            if (forceSplit || streamSize >= streamSplitSizeThreshold) {
                streamObjectCfList.add(writeStreamObject(streamRecords, streamSize).thenAccept(so -> {
                    synchronized (request) {
                        request.addStreamObject(so);
                    }
                }));
            } else {
                streamSetWriteCfList.add(limiter.acquire(streamSize).thenAccept(nil -> streamSetObject.write(streamId, streamRecords)));
                long startOffset = streamRecords.get(0).getBaseOffset();
                long endOffset = streamRecords.get(streamRecords.size() - 1).getLastOffset();
                request.addStreamRange(new ObjectStreamRange(streamId, -1L, startOffset, endOffset, streamSize));
            }
        }
        request.setObjectId(objectId);
        request.setOrderId(objectId);
        CompletableFuture<Void> streamSetObjectCf = CompletableFuture.allOf(streamSetWriteCfList.toArray(new CompletableFuture[0]))
            .thenCompose(nil -> streamSetObject.close().thenAccept(nil2 -> request.setObjectSize(streamSetObject.size())));
        List<CompletableFuture<?>> allCf = new LinkedList<>(streamObjectCfList);
        allCf.add(streamSetObjectCf);
        CompletableFuture.allOf(allCf.toArray(new CompletableFuture[0])).thenAccept(nil -> {
            commitStreamSetObjectRequest = request;
            uploadCf.complete(request);
        }).exceptionally(ex -> {
            uploadCf.completeExceptionally(ex);
            return null;
        });
    }

    public CompletableFuture<Void> commit() {
        return uploadCf.thenCompose(request -> objectManager.commitStreamSetObject(request).thenAccept(resp -> {
            LOGGER.info("Upload delta WAL {}, cost {}ms, rate limiter {}bytes/s", commitStreamSetObjectRequest,
                System.currentTimeMillis() - startTimestamp, rate);
            if (s3ObjectLogEnable) {
                s3ObjectLogger.trace("{}", commitStreamSetObjectRequest);
            }
        }).whenComplete((nil, ex) -> limiter.close()));
    }

    private CompletableFuture<StreamObject> writeStreamObject(List<StreamRecordBatch> streamRecords, int streamSize) {
        CompletableFuture<Long> cf = objectManager.prepareObject(1, TimeUnit.MINUTES.toMillis(60));
        cf = cf.thenCompose(objectId -> limiter.acquire(streamSize).thenApply(nil -> objectId));
        return cf.thenComposeAsync(objectId -> {
            ObjectWriter streamObjectWriter = ObjectWriter.writer(objectId, s3Operator, objectBlockSize, objectPartSize);
            long streamId = streamRecords.get(0).getStreamId();
            streamObjectWriter.write(streamId, streamRecords);
            long startOffset = streamRecords.get(0).getBaseOffset();
            long endOffset = streamRecords.get(streamRecords.size() - 1).getLastOffset();
            StreamObject streamObject = new StreamObject();
            streamObject.setObjectId(objectId);
            streamObject.setStreamId(streamId);
            streamObject.setStartOffset(startOffset);
            streamObject.setEndOffset(endOffset);
            return streamObjectWriter.close().thenApply(nil -> {
                streamObject.setObjectSize(streamObjectWriter.size());
                return streamObject;
            });
        }, executor);
    }

    public static class Builder {
        private Config config;
        private Map<Long, List<StreamRecordBatch>> streamRecordsMap;
        private ObjectManager objectManager;
        private S3Operator s3Operator;
        private ExecutorService executor;
        private Boolean forceSplit;
        private double rate = Long.MAX_VALUE;

        public Builder config(Config config) {
            this.config = config;
            return this;
        }

        public Builder streamRecordsMap(Map<Long, List<StreamRecordBatch>> streamRecordsMap) {
            this.streamRecordsMap = streamRecordsMap;
            return this;
        }

        public Builder objectManager(ObjectManager objectManager) {
            this.objectManager = objectManager;
            return this;
        }

        public Builder s3Operator(S3Operator s3Operator) {
            this.s3Operator = s3Operator;
            return this;
        }

        public Builder executor(ExecutorService executor) {
            this.executor = executor;
            return this;
        }

        public Builder forceSplit(boolean forceSplit) {
            this.forceSplit = forceSplit;
            return this;
        }

        public Builder rate(double rate) {
            this.rate = rate;
            return this;
        }

        public DeltaWALUploadTask build() {
            if (forceSplit == null) {
                boolean forceSplit = streamRecordsMap.size() == 1;
                if (!forceSplit) {
                    Optional<Boolean> hasStreamSetData = streamRecordsMap.values()
                        .stream()
                        .map(records -> records.stream().mapToLong(StreamRecordBatch::size).sum() >= config.streamSplitSize())
                        .filter(split -> !split)
                        .findAny();
                    if (hasStreamSetData.isEmpty()) {
                        forceSplit = true;
                    }
                }
                this.forceSplit = forceSplit;
            }
            return new DeltaWALUploadTask(config, streamRecordsMap, objectManager, s3Operator, executor, forceSplit, rate);
        }
    }

}
