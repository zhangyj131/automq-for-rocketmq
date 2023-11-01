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

package com.automq.rocketmq.common.config;

import java.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProxyConfig extends BaseConfig {
    private static final Logger LOGGER = LoggerFactory.getLogger(ProxyConfig.class);

    private String name;

    private String hostName;

    // The proportion of messages that are popped from the retry queue first,
    // default is 20, available value from 0 to 100.
    private int retryPriorityPercentage = 20;

    // lock expire time, default is 15min, unit in milliseconds.
    private long lockExpireTime = Duration.ofMinutes(15).toMillis();

    private int grpcThreadPoolNums = 16 + PROCESSOR_NUMBER * 2;
    private int grpcThreadPoolQueueCapacity = 100000;
    private int grpcListenPort = 8081;
    private int remotingListenPort = 8080;
    private int grpcBossLoopNum = 1;
    private int grpcWorkerLoopNum = PROCESSOR_NUMBER * 2;
    private boolean enableGrpcEpoll = false;
    private long channelExpiredTimeout = 1000 * 120;
    private boolean enablePrintJstack = true;
    private long printJstackInMillis = Duration.ofSeconds(60).toMillis();
    private long printThreadPoolStatusInMillis = Duration.ofSeconds(3).toMillis();

    /**
     * gRPC max message size
     * 130M = 4M * 32 messages + 2M attributes
     */
    private int grpcMaxInboundMessageSize = 130 * 1024 * 1024;
    private long grpcClientIdleTimeMills = Duration.ofSeconds(120).toMillis();

    public String name() {
        return name;
    }

    public String hostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public int retryPriorityPercentage() {
        return retryPriorityPercentage;
    }

    public long lockExpireTime() {
        return lockExpireTime;
    }

    public int grpcThreadPoolNums() {
        return grpcThreadPoolNums;
    }

    public int grpcThreadPoolQueueCapacity() {
        return grpcThreadPoolQueueCapacity;
    }

    public int getGrpcListenPort() {
        return grpcListenPort;
    }

    public void setGrpcListenPort(int grpcListenPort) {
        this.grpcListenPort = grpcListenPort;
    }

    public int grpcBossLoopNum() {
        return grpcBossLoopNum;
    }

    public int grpcWorkerLoopNum() {
        return grpcWorkerLoopNum;
    }

    public boolean enableGrpcEpoll() {
        return enableGrpcEpoll;
    }

    public int grpcMaxInboundMessageSize() {
        return grpcMaxInboundMessageSize;
    }

    public long grpcClientIdleTimeMills() {
        return grpcClientIdleTimeMills;
    }

    public long channelExpiredTimeout() {
        return channelExpiredTimeout;
    }

    public int grpcListenPort() {
        return grpcListenPort;
    }

    public boolean enablePrintJstack() {
        return enablePrintJstack;
    }

    public long printJstackInMillis() {
        return printJstackInMillis;
    }

    public long printThreadPoolStatusInMillis() {
        return printThreadPoolStatusInMillis;
    }

    public int remotingListenPort() {
        return remotingListenPort;
    }

    public void setRemotingListenPort(int remotingListenPort) {
        this.remotingListenPort = remotingListenPort;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setRetryPriorityPercentage(int retryPriorityPercentage) {
        this.retryPriorityPercentage = retryPriorityPercentage;
    }

    public void setLockExpireTime(long lockExpireTime) {
        this.lockExpireTime = lockExpireTime;
    }

    public void setGrpcThreadPoolNums(int grpcThreadPoolNums) {
        this.grpcThreadPoolNums = grpcThreadPoolNums;
    }

    public void setGrpcThreadPoolQueueCapacity(int grpcThreadPoolQueueCapacity) {
        this.grpcThreadPoolQueueCapacity = grpcThreadPoolQueueCapacity;
    }

    public void setGrpcBossLoopNum(int grpcBossLoopNum) {
        this.grpcBossLoopNum = grpcBossLoopNum;
    }

    public void setGrpcWorkerLoopNum(int grpcWorkerLoopNum) {
        this.grpcWorkerLoopNum = grpcWorkerLoopNum;
    }

    public void setEnableGrpcEpoll(boolean enableGrpcEpoll) {
        this.enableGrpcEpoll = enableGrpcEpoll;
    }

    public void setChannelExpiredTimeout(long channelExpiredTimeout) {
        this.channelExpiredTimeout = channelExpiredTimeout;
    }

    public void setEnablePrintJstack(boolean enablePrintJstack) {
        this.enablePrintJstack = enablePrintJstack;
    }

    public void setPrintJstackInMillis(long printJstackInMillis) {
        this.printJstackInMillis = printJstackInMillis;
    }

    public void setPrintThreadPoolStatusInMillis(long printThreadPoolStatusInMillis) {
        this.printThreadPoolStatusInMillis = printThreadPoolStatusInMillis;
    }

    public void setGrpcMaxInboundMessageSize(int grpcMaxInboundMessageSize) {
        this.grpcMaxInboundMessageSize = grpcMaxInboundMessageSize;
    }

    public void setGrpcClientIdleTimeMills(long grpcClientIdleTimeMills) {
        this.grpcClientIdleTimeMills = grpcClientIdleTimeMills;
    }
}
