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
import com.hortonworks.iotas.common.IotasEventImpl;
import com.hortonworks.iotas.common.Result;
import com.hortonworks.iotas.common.errors.ProcessingException;
import com.hortonworks.iotas.layout.design.component.Stream;
import com.hortonworks.iotas.processor.ProcessorRuntime;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Processor to join all sub-events for a root-event and emit to the output stream.
 */
public class JoinProcessorRuntime implements ProcessorRuntime {

    private Set<String> incomingStreams = new HashSet<>();
    private Map<String, EventGroup> groupedEvents = new HashMap<>();
    protected Stream outputStream;

    public JoinProcessorRuntime() {
    }

    public Stream getOutputStream() {
        return outputStream;
    }

    public void setOutputStream(Stream outputStream) {
        this.outputStream = outputStream;
    }

    @Override
    public List<Result> process(IotasEvent iotasEvent) throws ProcessingException {
        // group received event
        final EventGroup eventGroup = groupEvents(iotasEvent);

        // join them if group is complete
        if (eventGroup != null) {
            return Collections.singletonList(joinEvents(eventGroup));
        }

        return null;
    }

    @Override
    public void initialize(Map<String, Object> config) {

    }

    @Override
    public void cleanup() {

    }

    public void addIncomingStream(String stream) {
        incomingStreams.add(stream);
    }

    /**
     * Join all subevents and generate an event for the given output stream.
     *
     * @param eventGroup
     */
    protected Result joinEvents(EventGroup eventGroup) {
        Map<String, Object> fieldValues = new HashMap<>();
        Map<String, Object> auxiliaryFieldValues = new HashMap<>();
        for (PartitionedEvent subEvent : eventGroup.getPartitionedEvents()) {
            if(subEvent.getAuxiliaryFieldsAndValues() != null) {
                auxiliaryFieldValues.putAll(subEvent.getAuxiliaryFieldsAndValues());
            }
            if(subEvent.getFieldsAndValues() != null) {
                fieldValues.putAll(subEvent.getFieldsAndValues());
            }
        }

        IotasEventImpl joinedEvent = new IotasEventImpl(fieldValues, eventGroup.getGroupRootEvent().getDataSourceId(),
                UUID.randomUUID().toString(), eventGroup.getGroupRootEvent().getHeader(), this.outputStream.getId(), auxiliaryFieldValues);
        return new Result(this.outputStream.getId(), Collections.singletonList((IotasEvent) joinedEvent));
    }

    protected EventGroup groupEvents(IotasEvent iotasEvent) {
        EventGroup eventGroup = null;
        if (iotasEvent instanceof GroupRootEvent) {
            GroupRootEvent groupRootEvent = (GroupRootEvent) iotasEvent;
            eventGroup = getGroupedEvents(groupRootEvent.groupId);
            eventGroup.setGroupRootEvent(groupRootEvent);
        } else if (iotasEvent instanceof PartitionedEvent) {
            PartitionedEvent partitionedEvent = (PartitionedEvent) iotasEvent;
            eventGroup = getGroupedEvents(partitionedEvent.groupId);
            eventGroup.addPartitionEvent(partitionedEvent);
        }

        return eventGroup != null && eventGroup.isComplete() ? eventGroup : null;
    }

    private EventGroup getGroupedEvents(String groupId) {
        EventGroup eventGroup = this.groupedEvents.get(groupId);
        if (eventGroup == null) {
            eventGroup = new EventGroup();
            this.groupedEvents.put(groupId, eventGroup);
        }
        return eventGroup;
    }

}
