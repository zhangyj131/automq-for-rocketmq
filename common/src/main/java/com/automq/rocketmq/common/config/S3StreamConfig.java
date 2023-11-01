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

public class S3StreamConfig {
    private String s3Endpoint;
    private String s3Region = "cn-hangzhou";
    private String s3Bucket;
    private boolean s3ForcePathStyle;
    private String s3WALPath = "/tmp/s3stream_wal";
    private String s3AccessKey;
    private String s3SecretKey;

    public String s3Endpoint() {
        return s3Endpoint;
    }

    public String s3Region() {
        return s3Region;
    }

    public String s3Bucket() {
        return s3Bucket;
    }

    public boolean s3ForcePathStyle() {
        return s3ForcePathStyle;
    }

    public String s3WALPath() {
        return s3WALPath;
    }

    public String s3AccessKey() {
        return s3AccessKey;
    }

    public String s3SecretKey() {
        return s3SecretKey;
    }

    public void setS3Endpoint(String s3Endpoint) {
        this.s3Endpoint = s3Endpoint;
    }

    public void setS3Region(String s3Region) {
        this.s3Region = s3Region;
    }

    public void setS3Bucket(String s3Bucket) {
        this.s3Bucket = s3Bucket;
    }

    public void setS3ForcePathStyle(boolean s3ForcePathStyle) {
        this.s3ForcePathStyle = s3ForcePathStyle;
    }

    public void setS3WALPath(String s3WALPath) {
        this.s3WALPath = s3WALPath;
    }

    public void setS3AccessKey(String s3AccessKey) {
        this.s3AccessKey = s3AccessKey;
    }

    public void setS3SecretKey(String s3SecretKey) {
        this.s3SecretKey = s3SecretKey;
    }
}
