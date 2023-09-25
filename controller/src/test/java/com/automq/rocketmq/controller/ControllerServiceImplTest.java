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

package com.automq.rocketmq.controller;

import apache.rocketmq.controller.v1.Code;
import apache.rocketmq.controller.v1.ControllerServiceGrpc;
import apache.rocketmq.controller.v1.HeartbeatReply;
import apache.rocketmq.controller.v1.HeartbeatRequest;
import apache.rocketmq.controller.v1.ListTopicsReply;
import apache.rocketmq.controller.v1.ListTopicsRequest;
import apache.rocketmq.controller.v1.NodeRegistrationReply;
import apache.rocketmq.controller.v1.NodeRegistrationRequest;
import com.automq.rocketmq.controller.metadata.ControllerClient;
import com.automq.rocketmq.controller.metadata.ControllerConfig;
import com.automq.rocketmq.controller.metadata.DatabaseTestBase;
import com.automq.rocketmq.controller.metadata.database.DefaultMetadataStore;
import com.automq.rocketmq.controller.metadata.database.dao.Node;
import com.automq.rocketmq.controller.metadata.database.dao.Topic;
import com.automq.rocketmq.controller.metadata.database.dao.TopicStatus;
import com.automq.rocketmq.controller.metadata.database.mapper.NodeMapper;
import com.automq.rocketmq.controller.metadata.database.mapper.TopicMapper;
import io.grpc.Grpc;
import io.grpc.InsecureChannelCredentials;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ibatis.session.SqlSession;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

public class ControllerServiceImplTest extends DatabaseTestBase {

    @Test
    public void testRegisterBrokerNode() throws IOException {
        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.nodeId()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(1);

        ControllerClient client = Mockito.mock(ControllerClient.class);

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            ControllerServiceImpl svc = new ControllerServiceImpl(metadataStore);
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);
            NodeRegistrationRequest request = NodeRegistrationRequest.newBuilder()
                .setBrokerName("broker-name")
                .setAddress("localhost:1234")
                .setInstanceId("i-reg-broker")
                .build();

            AtomicInteger nodeId = new AtomicInteger(0);

            StreamObserver<NodeRegistrationReply> observer = new StreamObserver<NodeRegistrationReply>() {
                @Override
                public void onNext(NodeRegistrationReply reply) {
                    nodeId.set(reply.getId());
                }

                @Override
                public void onError(Throwable t) {
                    Assertions.fail(t);
                }

                @Override
                public void onCompleted() {

                }
            };

            svc.registerNode(request, observer);

            // It should work if register the broker multiple times. The only side effect is epoch is incremented.
            svc.registerNode(request, observer);

            try (SqlSession session = getSessionFactory().openSession()) {
                NodeMapper mapper = session.getMapper(NodeMapper.class);
                mapper.delete(nodeId.get());
                session.commit();
            }
        }
    }

    @Test
    public void testRegisterBroker_BadRequest() throws IOException {
        ControllerConfig config = Mockito.mock(ControllerConfig.class);
        Mockito.when(config.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(config.nodeId()).thenReturn(1);
        Mockito.when(config.leaseLifeSpanInSecs()).thenReturn(1);

        ControllerClient client = Mockito.mock(ControllerClient.class);

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(client, getSessionFactory(), config)) {
            metadataStore.start();
            ControllerServiceImpl svc = new ControllerServiceImpl(metadataStore);
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);
            NodeRegistrationRequest request = NodeRegistrationRequest.newBuilder()
                .setBrokerName("")
                .setAddress("localhost:1234")
                .setInstanceId("i-reg-broker")
                .build();

            StreamObserver<NodeRegistrationReply> observer = new StreamObserver() {
                @Override
                public void onNext(Object value) {
                    Assertions.fail("Should have raised an exception");
                }

                @Override
                public void onError(Throwable t) {
                    // OK, it's expected.
                }

                @Override
                public void onCompleted() {
                    Assertions.fail("Should have raised an exception");
                }
            };
            svc.registerNode(request, observer);
        }
    }

    @Test
    public void testHeartbeatGrpc() throws IOException {
        int nodeId;
        try (SqlSession session = getSessionFactory().openSession()) {
            NodeMapper mapper = session.getMapper(NodeMapper.class);
            Node node = new Node();
            node.setName("b1");
            node.setVolumeId("v-1");
            node.setVpcId("vpc-1");
            node.setAddress("localhost:1234");
            node.setInstanceId("i-1");
            node.setEpoch(1);
            mapper.create(node);
            nodeId = node.getId();
            session.commit();
        }

        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        ControllerConfig controllerConfig = Mockito.mock(ControllerConfig.class);
        Mockito.when(controllerConfig.nodeId()).thenReturn(1);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(controllerConfig.leaseLifeSpanInSecs()).thenReturn(1);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), controllerConfig)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore))) {
                testServer.start();
                int port = testServer.getPort();
                ManagedChannel channel = Grpc.newChannelBuilderForAddress("localhost", port, InsecureChannelCredentials.create()).build();
                ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub = ControllerServiceGrpc.newBlockingStub(channel);
                HeartbeatRequest request = HeartbeatRequest.newBuilder().setId(nodeId).setEpoch(1).build();
                HeartbeatReply reply = blockingStub.heartbeat(request);
                Assertions.assertEquals(Code.OK, reply.getStatus().getCode());
                channel.shutdownNow();
            }
        }
    }

    @Test
    public void testListTopics() throws IOException {

        try (SqlSession session = getSessionFactory().openSession()) {
            TopicMapper topicMapper = session.getMapper(TopicMapper.class);
            for (int i = 0; i < 3; i++) {
                Topic topic = new Topic();
                topic.setStatus(TopicStatus.ACTIVE);
                topic.setName("T" + i);
                topicMapper.create(topic);
            }
            session.commit();
        }

        ControllerClient controllerClient = Mockito.mock(ControllerClient.class);
        ControllerConfig controllerConfig = Mockito.mock(ControllerConfig.class);
        Mockito.when(controllerConfig.nodeId()).thenReturn(1);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);
        Mockito.when(controllerConfig.leaseLifeSpanInSecs()).thenReturn(2);
        Mockito.when(controllerConfig.scanIntervalInSecs()).thenReturn(1);

        try (DefaultMetadataStore metadataStore = new DefaultMetadataStore(controllerClient, getSessionFactory(), controllerConfig)) {
            metadataStore.start();
            Awaitility.await().with().pollInterval(100, TimeUnit.MILLISECONDS)
                .atMost(10, TimeUnit.SECONDS)
                .until(metadataStore::isLeader);

            try (ControllerTestServer testServer = new ControllerTestServer(0, new ControllerServiceImpl(metadataStore))) {
                testServer.start();
                int port = testServer.getPort();
                ManagedChannel channel = Grpc.newChannelBuilderForAddress("localhost", port, InsecureChannelCredentials.create()).build();
                ControllerServiceGrpc.ControllerServiceBlockingStub blockingStub = ControllerServiceGrpc.newBlockingStub(channel);
                Iterator<ListTopicsReply> replies = blockingStub.listAllTopics(ListTopicsRequest.newBuilder().build());
                while (replies.hasNext()) {
                    ListTopicsReply reply = replies.next();
                    Assertions.assertTrue(reply.getTopic().getName().startsWith("T"));
                    Assertions.assertEquals(Code.OK, reply.getStatus().getCode());
                }
                channel.shutdownNow();
            }
        }
    }

}