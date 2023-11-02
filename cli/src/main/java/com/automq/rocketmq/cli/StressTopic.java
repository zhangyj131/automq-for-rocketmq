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
import apache.rocketmq.controller.v1.DescribeTopicRequest;
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

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@CommandLine.Command(name = "stressTopci", mixinStandardHelpOptions = true, showDefaultValues = true)
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

    @CommandLine.Option(names = {"--ttl"}, description = "Time to live of the topic")
    String ttl = "3d0h";

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


        for (int i = 0; i < topicNums; i++) {
            long retentionHours = 0;
            try {
                retentionHours = DurationUtil.parse(this.ttl).toHours();
                if (retentionHours > Integer.MAX_VALUE) {
                    System.err.println("Invalid ttl: " + this.ttl + ", max value is 2147483647h");
                    System.exit(1);
                }
                if (retentionHours < 1) {
                    System.err.println("Invalid ttl: " + this.ttl + ", min value is 1h");
                    System.exit(1);
                }
            } catch (Exception e) {
                System.err.println("Invalid ttl: " + this.ttl);
                System.exit(1);
            }

            String topicName = topicPrefix + i;
            CreateTopicRequest request = CreateTopicRequest.newBuilder()
                .setTopic(topicName)
                .setCount(queueNums)
                .setAcceptTypes(AcceptTypes.newBuilder().addTypes(messageType).build())
                .setRetentionHours((int) retentionHours)
                .build();

            Long topicId = client.createTopic(mqAdmin.endpoint, request).join();
            System.out.println("Topic created: " + topicId);
            Topic topic = client.describeTopic(mqAdmin.endpoint, topicId, topicName).join();
            System.out.println(topic);
        }
        return null;
    }
}
