/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.automq.rocketmq.cli;

import apache.rocketmq.controller.v1.AcceptTypes;
import apache.rocketmq.controller.v1.CommitStreamObjectReply;
import apache.rocketmq.controller.v1.CommitStreamObjectRequest;
import apache.rocketmq.controller.v1.CommitWALObjectReply;
import apache.rocketmq.controller.v1.CommitWALObjectRequest;
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.OpenStreamReply;
import apache.rocketmq.controller.v1.OpenStreamRequest;
import apache.rocketmq.controller.v1.PrepareS3ObjectsReply;
import apache.rocketmq.controller.v1.PrepareS3ObjectsRequest;
import apache.rocketmq.controller.v1.StreamMetadata;
import apache.rocketmq.controller.v1.SubStream;
import apache.rocketmq.controller.v1.SubStreams;
import com.automq.rocketmq.common.PrefixThreadFactory;
import com.automq.rocketmq.controller.ControllerClient;
import com.automq.rocketmq.controller.client.GrpcControllerClient;
import apache.rocketmq.controller.v1.S3StreamObject;
import apache.rocketmq.controller.v1.S3WALObject;

import com.automq.rocketmq.controller.exception.ControllerException;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import picocli.CommandLine;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@CommandLine.Command(name = "stressController", mixinStandardHelpOptions = true, showDefaultValues = true)
public class StressController implements Callable<Void> {

    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @CommandLine.Option(names = {"-t", "--streamNums"}, description = "Number of s3streams")
    int streamNums = 5;

    @CommandLine.Option(names = {"-t", "--walNums"}, description = "Number of s3wals")
    int walNums = 5;

    @CommandLine.Option(names = {"-m", "--BatchNums"}, description = "Number of streamMetadata batch")
    int batchNums = Integer.MAX_VALUE;

    @CommandLine.Option(names = {"-tp", "--topicPrefix"}, description = "The prefix of the created topics")
    String topicPrefix = "Benchmark_Topic_";

    @CommandLine.Option(names = {"-mt", "--messageType"}, description = "Message type")
    MessageType messageType = MessageType.NORMAL;

    @CommandLine.Option(names = {"-d", "--duration"}, description = "Duration in seconds")
    int durationInSeconds = 10;

    @CommandLine.Option(names = {"-i", "--reportIntervalInSeconds"}, description = "Report interval in seconds")
    int reportIntervalInSeconds = 3;

    private final MetricRegistry metrics = new MetricRegistry();

    @Override
    public Void call() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool(new PrefixThreadFactory("Benchmark-Controller"));
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
            .build();
        reporter.start(reportIntervalInSeconds, TimeUnit.SECONDS);
        Timer timer = metrics.timer("Timer for sending messages");
        long startTimestamp = System.currentTimeMillis();
        ControllerClient client = new GrpcControllerClient(new CliClientConfig());
        prepareTopicAndStream(client);

        executor.submit(() -> {
            for (int i = 0; i < batchNums; i++) {
                if (System.currentTimeMillis() - startTimestamp > durationInSeconds * 1000L) {
                    break;
                }
                try {
                    long start = System.currentTimeMillis();
                    List<S3StreamObject> s3StreamObjects = new ArrayList<>();
                    List<Long> compactedObjectIds = new ArrayList<>();

                    System.out.println("Start stress controller...");
                    long startOffset = commitS3Stream(client, compactedObjectIds, 0);
                    commitS3Wal(client, compactedObjectIds, startOffset);
                    System.out.println("Start commit all wal...");
                    PrepareS3ObjectsRequest prepareS3ObjectsRequest = PrepareS3ObjectsRequest.newBuilder()
                        .setPreparedCount(1)
                        .setTimeToLiveMinutes(1)
                        .build();

                    PrepareS3ObjectsReply prepareS3ObjectsReply = client.prepareS3Objects(mqAdmin.endpoint, prepareS3ObjectsRequest).join();
                    long objectId = prepareS3ObjectsReply.getFirstObjectId();
                    System.out.println("S3 objects prepared, first objectId: " + objectId);

                    CommitWALObjectRequest request = CommitWALObjectRequest.newBuilder()
                        .setS3WalObject(buildS3WalObjs(objectId, 1).get(0))
                        .addAllS3StreamObjects(s3StreamObjects)
                        .addAllCompactedObjectIds(compactedObjectIds)
                        .build();

                    CommitWALObjectReply walObjectReply = client.commitWALObject(mqAdmin.endpoint, request).join();
                    System.out.println("WAL committed: " + walObjectReply.getStatus());
                    timer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                } catch (Exception e) {
                    System.out.println("Failed to stress controller: " + e.getMessage());
                }
            }
        });

        executor.awaitTermination(durationInSeconds + 1, TimeUnit.SECONDS);
        reporter.close();

        return null;
    }

    private void commitS3Wal(ControllerClient client, List<Long> compactedObjectIds, long startOffset) {
        System.out.println("Start commit wal...");
        for (int i = 0; i < walNums; i++) {
            PrepareS3ObjectsRequest prepareS3ObjectsRequest = PrepareS3ObjectsRequest.newBuilder()
                .setPreparedCount(1)
                .setTimeToLiveMinutes(1)
                .build();

            PrepareS3ObjectsReply prepareS3ObjectsReply = client.prepareS3Objects(mqAdmin.endpoint, prepareS3ObjectsRequest).join();
            long objectId = prepareS3ObjectsReply.getFirstObjectId();
            System.out.println("S3 objects prepared, first objectId: " + objectId);

            List<S3WALObject> s3WalObjects = buildS3WalObjs(objectId, 1).stream()
                .map(s3WALObject -> S3WALObject.newBuilder(s3WALObject)
                    .setSubStreams(SubStreams.newBuilder()
                        .putAllSubStreams(buildWalSubStreams(2, startOffset, 100)).build())
                    .build()).toList();

            CommitWALObjectRequest request = CommitWALObjectRequest.newBuilder()
                .setS3WalObject(s3WalObjects.get(0))
                .addAllS3StreamObjects(Collections.emptyList())
                .addAllCompactedObjectIds(Collections.emptyList())
                .build();
            CommitWALObjectReply walObjectReply = client.commitWALObject(mqAdmin.endpoint, request).join();
            System.out.println("WAL committed: " + walObjectReply.getStatus());
            compactedObjectIds.add(objectId);
        }
    }

    private long commitS3Stream(ControllerClient client, List<Long> compactedObjectIds, long startOffset) {
        System.out.println("Start commit stream...");
        for (int i = 0; i < streamNums; i++) {
            PrepareS3ObjectsRequest prepareS3ObjectsRequest = PrepareS3ObjectsRequest.newBuilder()
                .setPreparedCount(1)
                .setTimeToLiveMinutes(1)
                .build();

            PrepareS3ObjectsReply prepareS3ObjectsReply = client.prepareS3Objects(mqAdmin.endpoint, prepareS3ObjectsRequest).join();
            long objectId = prepareS3ObjectsReply.getFirstObjectId();
            System.out.println("S3 objects prepared, first objectId: " + objectId);

            List<S3StreamObject> streamObjects = buildS3StreamObjs(objectId, 1, startOffset, 100);
            CommitStreamObjectRequest request = CommitStreamObjectRequest.newBuilder()
                .setS3StreamObject(streamObjects.get(0))
                .addAllCompactedObjectIds(Collections.emptyList())
                .build();
            startOffset = streamObjects.get(0).getEndOffset();
            CommitStreamObjectReply streamObjectReply = client.commitStreamObject(mqAdmin.endpoint, request).join();
            System.out.println("Stream committed: " + streamObjectReply.getStatus());
            compactedObjectIds.add(objectId);
        }
        return startOffset;
    }

    private void prepareTopicAndStream(ControllerClient client) throws ControllerException {
        System.out.printf("Prepare topic and init stream");
        String topic = topicPrefix + 1;
        CreateTopicRequest createTopicRequest = CreateTopicRequest.newBuilder()
            .setTopic(topic)
            .setCount(2)
            .setAcceptTypes(AcceptTypes.newBuilder().addTypes(messageType).build())
            .build();

        Long topicId = client.createTopic(mqAdmin.endpoint, createTopicRequest).join();
        System.out.println("Topic created: " + topicId);

        // TODO: wait api which can list streams by topicId, queueId;
        long streamId = 1L;
        System.out.println("Start open Stream: ");
        OpenStreamRequest openStreamRequest = OpenStreamRequest.newBuilder()
            .setStreamId(streamId)
            .setStreamEpoch(-1)
            .build();

        OpenStreamReply reply = client.openStream(mqAdmin.endpoint, openStreamRequest).join();
        StreamMetadata metadata = reply.getStreamMetadata();
        System.out.println("Stream opened: " + metadata);
    }

    private List<S3StreamObject> buildS3StreamObjs(long objectId,
        int count, long startOffset, long interval) {
        List<S3StreamObject> s3StreamObjects = new ArrayList<>();

        for (long i = 0; i < count; i++) {
            S3StreamObject streamObject = S3StreamObject.newBuilder()
                .setObjectId(objectId + i)
                .setObjectSize(100 + i)
                .setStreamId(i + 1)
                .setStartOffset(startOffset + i * interval)
                .setEndOffset(startOffset + (i + 1) * interval)
                .setBaseDataTimestamp(System.currentTimeMillis())
                .build();

            s3StreamObjects.add(streamObject);
        }

        return s3StreamObjects;
    }

    private List<S3WALObject> buildS3WalObjs(long objectId, int count) {
        List<S3WALObject> s3WALObjects = new ArrayList<>();

        for (long i = 0; i < count; i++) {
            S3WALObject walObject = S3WALObject.newBuilder()
                .setObjectId(objectId + i)
                .setObjectSize(100 + i)
                .setSequenceId(objectId + i)
                .setBrokerId((int) i + 1)
                .setBaseDataTimestamp(System.currentTimeMillis())
                .build();

            s3WALObjects.add(walObject);
        }

        return s3WALObjects;
    }

    private Map<Long, SubStream> buildWalSubStreams(int count, long startOffset, long interval) {
        Map<Long, SubStream> subStreams = new HashMap<>();
        for (int i = 0; i < count; i++) {
            SubStream subStream = SubStream.newBuilder()
                .setStreamId(i + 1)
                .setStartOffset(startOffset + i * interval)
                .setEndOffset(startOffset + (i + 1) * interval)
                .build();

            subStreams.put((long) i + 1, subStream);
        }
        return subStreams;
    }

}
