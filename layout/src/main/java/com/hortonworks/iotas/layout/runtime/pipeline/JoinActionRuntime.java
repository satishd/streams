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
import com.hortonworks.iotas.common.Result;
import com.hortonworks.iotas.layout.runtime.ActionRuntime;
import com.hortonworks.iotas.util.ProxyUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class JoinActionRuntime implements ActionRuntime {
    private Map<String, EventGroup> groupedEvents = new HashMap<>();
    private String outputStream;
    private final JoinAction joinAction;
    private Joiner joiner;

    public JoinActionRuntime(String outputStream, JoinAction joinAction) {
        this.outputStream = outputStream;
        this.joinAction = joinAction;
    }

    public void prepare() {
        final String jarId = joinAction.getJarId();
        final String splitterClassName = joinAction.getJoinerClassName();
        if(jarId != null || splitterClassName != null) {
            ProxyUtil<Joiner> proxyUtil = new ProxyUtil<>(Joiner.class, this.getClass().getClassLoader());
            try {
                joiner = proxyUtil.loadClassFromJar(jarId, splitterClassName);
            } catch (Exception e) {
                throw new RuntimeException(e.getMessage(), e);
            }
        } else {
            joiner = new DefaultJoiner(outputStream);
        }
    }

    @Override
    public List<Result> execute(IotasEvent iotasEvent) {
        // group received event if possible
        final EventGroup eventGroup = groupEvents(iotasEvent);

        // join them if group is complete
        if (eventGroup != null && eventGroup.isComplete()) {
            return Collections.singletonList(joinEvents(eventGroup));
        }

        return null;
    }

    /**
     * Join all subevents and generate an event for the given output stream.
     *
     * @param eventGroup
     */
    protected Result joinEvents(EventGroup eventGroup) {
        IotasEvent joinedEvent = joiner.join(eventGroup);
        return new Result(outputStream, Collections.singletonList(joinedEvent));
    }

    protected EventGroup groupEvents(IotasEvent iotasEvent) {
        EventGroup eventGroup = null;
        if (iotasEvent instanceof GroupRootEvent) { // todo use header instead of a separate event class
            GroupRootEvent groupRootEvent = (GroupRootEvent) iotasEvent;
            eventGroup = getGroupedEvents(groupRootEvent.groupId);
            eventGroup.setGroupRootEvent(groupRootEvent);
        } else if (iotasEvent instanceof PartitionedEvent) { // todo use header instead of a separate event class
            PartitionedEvent partitionedEvent = (PartitionedEvent) iotasEvent;
            eventGroup = getGroupedEvents(partitionedEvent.groupId);
            eventGroup.addPartitionEvent(partitionedEvent);
        }

        return eventGroup;
    }

    private EventGroup getGroupedEvents(String groupId) {
        EventGroup eventGroup = groupedEvents.get(groupId);
        if (eventGroup == null) {
            eventGroup = new EventGroup();
            groupedEvents.put(groupId, eventGroup);
        }
        return eventGroup;
    }

    @Override
    public List<String> getOutputStreams() {
        return null;
    }
}
