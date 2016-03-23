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
import com.hortonworks.iotas.common.errors.ProcessingException;
import com.hortonworks.iotas.layout.design.component.Stream;
import com.hortonworks.iotas.processor.ProcessorRuntime;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Split the payload based on a given criteria. It splits the payload and sends it to parallel stages. Each stage can have
 * List of Actions. Current supported Action is Enrichment and other actions like projection/filters etc can be added.
 * <p>
 * todo Take Splitter class from user and use that for getting groupId, splitting the payload, take output streams for
 * which these events should be sent etc. Implement a default identity splitter which returns the same event to given
 * output streams.
 * <p>
 * start with ui and make the abstractions based on that. It gives better clarity.
 */
public abstract class SplitProcessorRuntime implements ProcessorRuntime {

    public static final String ROOT_MESSAGE_STREAM = "root-message-stream";
    protected final List<Stream> outputStreams;

    protected SplitProcessorRuntime(List<Stream> outputStreams) {
        this.outputStreams = outputStreams;
    }

    @Override
    public void initialize(Map<String, Object> config) {

    }

    @Override
    public List<Result> process(IotasEvent iotasEvent) throws ProcessingException {
        return splitEvent(iotasEvent);
    }

    @Override
    public void cleanup() {

    }

    public abstract List<Result> splitEvent(IotasEvent iotasEvent);

    /**
     * @param iotasEvent
     * @return groupid for a given {@code iotasEvent}
     */
    protected String getGroupId(IotasEvent iotasEvent) {
        return UUID.randomUUID().toString();
    }


}
