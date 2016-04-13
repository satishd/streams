/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.hortonworks.iotas.layout.runtime.splitjoin;

import com.hortonworks.iotas.common.IotasEvent;

import java.util.Map;

/**
 * Partitioned event sent by split processor. It contains {@code groupId} and {@code partNo} of the split event.
 */
public class PartitionedEvent implements IotasEvent {

    private final IotasEvent iotasEvent;
    private final String streamId;
    protected String groupId;
    protected int partNo;

    public PartitionedEvent(IotasEvent iotasEvent, String streamId, String groupId, int partNo) {
        this.iotasEvent = iotasEvent;
        this.streamId = streamId;
        this.groupId = groupId;
        this.partNo = partNo;
    }

    @Override
    public Map<String, Object> getFieldsAndValues() {
        return iotasEvent.getFieldsAndValues();
    }

    @Override
    public Map<String, Object> getAuxiliaryFieldsAndValues() {
        return iotasEvent.getAuxiliaryFieldsAndValues();
    }

    @Override
    public void addAuxiliaryFieldAndValue(String field, Object value) {
        iotasEvent.addAuxiliaryFieldAndValue(field, value);
    }

    @Override
    public Map<String, Object> getHeader() {
        return iotasEvent.getHeader();
    }

    @Override
    public String getId() {
        return iotasEvent.getId();
    }

    @Override
    public String getDataSourceId() {
        return iotasEvent.getDataSourceId();
    }

    @Override
    public String getSourceStream() {
        return streamId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PartitionedEvent)) return false;

        PartitionedEvent that = (PartitionedEvent) o;

        if (partNo != that.partNo) return false;
        if (!iotasEvent.equals(that.iotasEvent)) return false;
        if (!streamId.equals(that.streamId)) return false;
        return groupId.equals(that.groupId);

    }

    @Override
    public int hashCode() {
        int result = iotasEvent.hashCode();
        result = 31 * result + streamId.hashCode();
        result = 31 * result + groupId.hashCode();
        result = 31 * result + partNo;
        return result;
    }
}
