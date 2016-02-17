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
package com.hortonworks.iotas.layout.runtime.pipelines;

import com.hortonworks.iotas.common.IotasEvent;
import com.hortonworks.iotas.layout.design.component.Processor;
import com.hortonworks.iotas.layout.runtime.ActionRuntime;

import java.util.*;

/**
 *
 */
public abstract class JoinProcessor extends Processor {

    private Set<String> incomingStreams = new HashSet<>();
    private Map<String, GroupInfo> groupedEvents = new HashMap<>();

    public JoinProcessor() {
    }

    public void addIncomingStream(String stream) {
        incomingStreams.add(stream);
    }

    protected ActionRuntime.Result joinEvents(Iterator<? extends IotasEvent> iotasEvents) {
        return null;
    }

    protected Iterator<? extends IotasEvent> groupEvents(IotasEvent iotasEvent) {
        GroupInfo groupedEvents = null;
        if(iotasEvent instanceof GroupRootEvent) {
            GroupRootEvent groupRootEvent = (GroupRootEvent) iotasEvent;
            groupedEvents = getGroupedEvents(groupRootEvent.groupId);
            groupedEvents.size = groupRootEvent.noOfMessages;
        } else if (iotasEvent instanceof PartitionedEvent) {
            PartitionedEvent partitionedEvent = (PartitionedEvent) iotasEvent;
            groupedEvents = getGroupedEvents(partitionedEvent.groupId);
        }

        return groupedEvents.isComplete() ? groupedEvents.partitionedEvents.values().iterator() : null;
    }

    private GroupInfo getGroupedEvents(String groupId) {
        GroupInfo groupInfo = groupedEvents.get(groupId);
        if(groupInfo == null) {
            groupInfo = new GroupInfo();
            groupedEvents.put(groupId, groupInfo);
        }
        return groupInfo;
    }

    public ActionRuntime.Result execute(IotasEvent iotasEvent) {
        // group received event
        Iterator<? extends IotasEvent> groupedEvents = groupEvents(iotasEvent);

        // join them if group is complete
        if(groupedEvents != null && groupedEvents.hasNext()) {
            return joinEvents(groupedEvents);
        }

        return null;
    }

    private static class GroupInfo {
        private int size = -1;
        private Map<Integer, PartitionedEvent> partitionedEvents = new HashMap<>();

        public void addPartitionEvent(PartitionedEvent partitionedEvent) {
            partitionedEvents.put(partitionedEvent.partNo, partitionedEvent);
        }

        public boolean isComplete() {
            return partitionedEvents.size() == size;
        }

    }

}
