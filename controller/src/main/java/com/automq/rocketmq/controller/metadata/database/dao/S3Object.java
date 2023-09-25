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

package com.automq.rocketmq.controller.metadata.database.dao;

import apache.rocketmq.controller.v1.S3ObjectState;

import java.util.Objects;

public class S3Object {

    long id;

    long objectId;

    long objectSize;
    
    long preparedTimestamp;

    long committedTimestamp;

    long expiredTimestamp;

    long markedForDeletionTimestamp;

    S3ObjectState state;

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getObjectId() {
        return objectId;
    }

    public void setObjectId(long objectId) {
        this.objectId = objectId;
    }

    public long getObjectSize() {
        return objectSize;
    }

    public void setObjectSize(long objectSize) {
        this.objectSize = objectSize;
    }

    public long getPreparedTimestamp() {
        return preparedTimestamp;
    }

    public void setPreparedTimestamp(long preparedTimestamp) {
        this.preparedTimestamp = preparedTimestamp;
    }

    public long getCommittedTimestamp() {
        return committedTimestamp;
    }

    public void setCommittedTimestamp(long committedTimestamp) {
        this.committedTimestamp = committedTimestamp;
    }

    public long getExpiredTimestamp() {
        return expiredTimestamp;
    }

    public void setExpiredTimestamp(long expiredTimestamp) {
        this.expiredTimestamp = expiredTimestamp;
    }

    public long getMarkedForDeletionTimestamp() {
        return markedForDeletionTimestamp;
    }

    public void setMarkedForDeletionTimestamp(long markedForDeletionTimestamp) {
        this.markedForDeletionTimestamp = markedForDeletionTimestamp;
    }

    public S3ObjectState getState() {
        return state;
    }

    public void setState(S3ObjectState state) {
        this.state = state;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        S3Object s3Object = (S3Object) o;
        return id == s3Object.id && objectId == s3Object.objectId && objectSize == s3Object.objectSize && preparedTimestamp == s3Object.preparedTimestamp && committedTimestamp == s3Object.committedTimestamp && expiredTimestamp == s3Object.expiredTimestamp && markedForDeletionTimestamp == s3Object.markedForDeletionTimestamp && state == s3Object.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, objectId, objectSize, preparedTimestamp, committedTimestamp, expiredTimestamp, markedForDeletionTimestamp, state);
    }
}
