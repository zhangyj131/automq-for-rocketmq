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
import apache.rocketmq.controller.v1.CreateTopicRequest;
import apache.rocketmq.controller.v1.MessageType;
import apache.rocketmq.controller.v1.Topic;
import com.automq.rocketmq.common.PrefixThreadFactory;
import com.automq.rocketmq.common.util.DurationUtil;
import com.automq.rocketmq.controller.ControllerClient;
import com.automq.rocketmq.controller.client.GrpcControllerClient;
import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import picocli.CommandLine;

import java.util.LinkedList;
import java.util.Objects;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@CommandLine.Command(name = "stressTopic", mixinStandardHelpOptions = true, showDefaultValues = true)
public class StressTopic implements Callable<Void> {

    @CommandLine.ParentCommand
    MQAdmin mqAdmin;

    @CommandLine.Option(names = {"-q", "--queueNums"}, description = "Number of queues per topic")
    int queueNums = 1;

    @CommandLine.Option(names = {"-m", "--topicNums"}, description = "Number of topics")
    int topicNums = Integer.MAX_VALUE;

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
        ExecutorService executor = Executors.newCachedThreadPool(new PrefixThreadFactory("Benchmark-Topic"));
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
        ConsoleReporter reporter = ConsoleReporter.forRegistry(metrics)
            .build();
        reporter.start(reportIntervalInSeconds, TimeUnit.SECONDS);
        Timer timer = metrics.timer("Timer for benchmark topic");
        long startTimestamp = System.currentTimeMillis();
        ControllerClient client = new GrpcControllerClient(new CliClientConfig());
        long retentionHours = DurationUtil.parse("1h").toHours();
        Random random = new Random();
        CountDownLatch latch = new CountDownLatch(topicNums);

        Queue<Long> topicIds = new LinkedList<>();

        executor.submit(() -> {
            for (int i = 0; i < topicNums; i++) {
                if (System.currentTimeMillis() - startTimestamp > durationInSeconds * 1000L) {
                    break;
                }

                try {
                    String topicName = topicPrefix + i + generateRandomSuffix(7, random);
                    CreateTopicRequest request = CreateTopicRequest.newBuilder()
                        .setTopic(topicName)
                        .setCount(queueNums)
                        .setAcceptTypes(AcceptTypes.newBuilder().addTypes(messageType).build())
                        .setRetentionHours((int) retentionHours)
                        .build();

                    Long topicId = client.createTopic(mqAdmin.endpoint, request).join();
                    topicIds.add(topicId);
                } catch (Exception e) {
                    System.out.println("Failed to stress Topic: " + e.getMessage());
                } finally {
                    long start = System.currentTimeMillis();
                    timer.update(System.currentTimeMillis() - start, TimeUnit.MILLISECONDS);
                    latch.countDown();
                }
            }
        });

        scheduler.scheduleAtFixedRate(() -> {
            try {
                if (!topicIds.isEmpty()) {
                    Long topicId = topicIds.poll();
                    Topic topic = client.describeTopic(mqAdmin.endpoint, topicId, null).join();
                    if (Objects.isNull(topic)) {
                        throw new RuntimeException("Topic not found: " + topicId);
                    }
                }
            } catch (Exception e) {
                System.out.println("Failed to describe topic: " + e.getMessage());
            }
        }, 0, 1, TimeUnit.SECONDS);

        latch.await();
        executor.awaitTermination(durationInSeconds + 1, TimeUnit.SECONDS);
        scheduler.awaitTermination(durationInSeconds + 1, TimeUnit.SECONDS);
        reporter.close();
        return null;
    }


    private String generateRandomSuffix(int length, Random random) {
        StringBuilder sb = new StringBuilder(length);
        String characters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        int charactersLength = characters.length();
        for (int i = 0; i < length; i++) {
            int randomIndex = random.nextInt(charactersLength);
            char randomChar = characters.charAt(randomIndex);
            sb.append(randomChar);
        }
        return sb.toString();
    }
}
