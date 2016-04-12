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
package com.hortonworks.iotas.layout.runtime.pipeline;

import com.hortonworks.iotas.common.IotasEvent;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class EventGroup {
    private final Map<Integer, IotasEvent> partitionedEvents = new HashMap<>();
    private final String groupId;
    private final String dataSourceId;

    private int totalPartitionEvents = -1;

    public EventGroup(String groupId, String dataSourceId) {
        this.groupId = groupId;
        this.dataSourceId = dataSourceId;
    }

    public void addPartitionEvent(IotasEvent partitionedEvent) {
        final Map<String, Object> header = partitionedEvent.getHeader();
        if(header == null || !header.containsKey(SplitActionRuntime.SPLIT_PARTITION_ID)) {
            throw new IllegalArgumentException("Received event is not of partition event as it doe not contain header  with name: "+SplitActionRuntime.SPLIT_PARTITION_ID);
        }
        partitionedEvents.put((Integer) header.get(SplitActionRuntime.SPLIT_PARTITION_ID), partitionedEvent);
    }

    public boolean isComplete() {
        return partitionedEvents.size() == totalPartitionEvents;
    }

    public String getDataSourceId() {
        return dataSourceId;
    }

    public Iterable<IotasEvent> getPartitionedEvents() {
        return Collections.unmodifiableCollection(partitionedEvents.values());
    }
}
