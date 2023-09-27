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

package com.automq.rocketmq.store.service;

import com.automq.rocketmq.common.model.FlatMessageExt;
import com.automq.rocketmq.metadata.StoreMetadataService;
import com.automq.rocketmq.store.api.StreamStore;
import com.automq.rocketmq.store.exception.StoreException;
import com.automq.rocketmq.store.mock.MockStoreMetadataService;
import com.automq.rocketmq.store.mock.MockStreamStore;
import com.automq.rocketmq.store.model.stream.SingleRecord;
import com.automq.rocketmq.store.service.api.KVService;
import com.automq.rocketmq.store.util.FlatMessageUtil;
import com.automq.rocketmq.store.util.SerializeUtil;
import com.automq.stream.api.FetchResult;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static com.automq.rocketmq.store.mock.MockMessageUtil.buildMessage;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ReviveServiceTest {
    private static final String PATH = "/tmp/test_revive_service/";
    protected static final String KV_NAMESPACE_CHECK_POINT = "check_point";
    protected static final String KV_NAMESPACE_TIMER_TAG = "timer_tag";

    private KVService kvService;
    private StoreMetadataService metadataService;
    private StreamStore streamStore;
    private InflightService inflightService;
    private ReviveService reviveService;

    @BeforeEach
    public void setUp() throws StoreException {
        kvService = new RocksDBKVService(PATH);
        metadataService = new MockStoreMetadataService();
        streamStore = new MockStreamStore();
        inflightService = new InflightService();
        reviveService = new ReviveService(KV_NAMESPACE_CHECK_POINT, KV_NAMESPACE_TIMER_TAG, kvService, metadataService, inflightService, streamStore);
    }

    @AfterEach
    public void tearDown() throws StoreException {
        kvService.destroy();
    }

    @Test
    void tryRevive() throws StoreException {
        // Append mock message.
        long streamId = metadataService.getStreamId(32, 32);
        streamStore.append(streamId, new SingleRecord(buildMessage(32, 32, ""))).join();

        // Append mock check point and timer tag.
        kvService.put(KV_NAMESPACE_CHECK_POINT, SerializeUtil.buildCheckPointKey(32, 32, 0, Long.MAX_VALUE), new byte[0]);
        kvService.put(KV_NAMESPACE_TIMER_TAG, SerializeUtil.buildTimerTagKey(0, 32, 32, 0, Long.MAX_VALUE),
            SerializeUtil.buildTimerTagValue(0, 32, 32, 32, streamId, 0, Long.MAX_VALUE));
        inflightService.increaseInflightCount(32, 32, 32, 1);

        kvService.put(KV_NAMESPACE_TIMER_TAG, SerializeUtil.buildTimerTagKey(Long.MAX_VALUE, 0, 0, 0, 0),
            SerializeUtil.buildTimerTagValue(Long.MAX_VALUE, 0, 0, 0, 0, 0, 0));
        inflightService.increaseInflightCount(0, 0, 0, 1);

        reviveService.tryRevive();

        assertEquals(1, inflightService.getInflightCount(0, 0, 0));

        long retryStreamId = metadataService.getRetryStreamId(32, 32, 32);
        FetchResult fetchResult = streamStore.fetch(retryStreamId, 0, 100).join();
        assertEquals(1, fetchResult.recordBatchList().size());

        FlatMessageExt messageExt = FlatMessageUtil.transferToMessageExt(fetchResult.recordBatchList().get(0));
        assertEquals(1, messageExt.reconsumeCount());
        assertEquals(32, messageExt.message().topicId());
        assertEquals(32, messageExt.message().queueId());
        assertEquals(0, messageExt.offset());

        AtomicInteger checkPointCount = new AtomicInteger();
        kvService.iterate(KV_NAMESPACE_CHECK_POINT, (key, value) -> checkPointCount.getAndIncrement());
        assertEquals(0, checkPointCount.get());

        AtomicInteger timerTagCount = new AtomicInteger();
        kvService.iterate(KV_NAMESPACE_TIMER_TAG, (key, value) -> timerTagCount.getAndIncrement());
        assertEquals(1, timerTagCount.get());

        // Append timer tag of retry message.
        long nextVisibleTimestamp = System.currentTimeMillis() - 100;
        kvService.put(KV_NAMESPACE_TIMER_TAG, SerializeUtil.buildTimerTagKey(nextVisibleTimestamp, 32, 32, 0, Long.MAX_VALUE),
            SerializeUtil.buildTimerTagValue(nextVisibleTimestamp, 32, 32, 32, retryStreamId, 0, Long.MAX_VALUE));
        inflightService.increaseInflightCount(32, 32, 32, 1);

        reviveService.tryRevive();

        assertEquals(1, inflightService.getInflightCount(0, 0, 0));

        long deadLetterStreamId = metadataService.getDeadLetterStreamId(32, 32, 32);
        fetchResult = streamStore.fetch(deadLetterStreamId, 0, 100).join();
        assertEquals(1, fetchResult.recordBatchList().size());

        messageExt = FlatMessageUtil.transferToMessageExt(fetchResult.recordBatchList().get(0));
        assertEquals(2, messageExt.reconsumeCount());
        assertEquals(32, messageExt.message().topicId());
        assertEquals(32, messageExt.message().queueId());
        assertEquals(0, messageExt.offset());

        timerTagCount.set(0);
        kvService.iterate(KV_NAMESPACE_TIMER_TAG, (key, value) -> timerTagCount.incrementAndGet());
        assertEquals(1, timerTagCount.get());
    }
}