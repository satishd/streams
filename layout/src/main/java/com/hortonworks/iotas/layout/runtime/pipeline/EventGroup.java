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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class EventGroup {
    private Map<Integer, PartitionedEvent> partitionedEvents = new HashMap<>();
    private GroupRootEvent groupRootEvent;

    public void addPartitionEvent(PartitionedEvent partitionedEvent) {
        partitionedEvents.put(partitionedEvent.partNo, partitionedEvent);
    }

    public boolean isComplete() {
        return groupRootEvent != null && partitionedEvents.size() == groupRootEvent.noOfMessages;
    }

    public void setGroupRootEvent(GroupRootEvent groupRootEvent) {
        this.groupRootEvent = groupRootEvent;
    }

    public GroupRootEvent getGroupRootEvent() {
        return groupRootEvent;
    }

    public Iterable<PartitionedEvent> getPartitionedEvents() {
        return Collections.unmodifiableCollection(partitionedEvents.values());
    }
}
