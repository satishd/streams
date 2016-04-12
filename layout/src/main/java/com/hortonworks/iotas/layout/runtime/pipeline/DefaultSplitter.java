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
import com.hortonworks.iotas.layout.design.component.Stream;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * This class broadcasts the received event to all the output streams. This can be extended to customize split logic.
 *
 */
public class DefaultSplitter implements Splitter {

    private final List<String> outputStreams;

    public DefaultSplitter(List<String> outputStreams) {
        this.outputStreams = outputStreams;
    }

    @Override
    public List<Result> splitEvent(IotasEvent iotasEvent) {
        List<Result> results = new ArrayList<>();
        String groupId = getGroupId(iotasEvent);
        int curPartNo = 0;
        for (String stream : outputStreams) {
            results.add(new Result(stream, Collections.singletonList((IotasEvent)
                    new PartitionedEvent(iotasEvent, stream, groupId, ++curPartNo))));
        }
//        results.add(new Result(ROOT_MESSAGE_STREAM, Collections.singletonList((IotasEvent) new GroupRootEvent(iotasEvent, groupId, curPartNo))));
        return results;
    }

    /**
     * @param iotasEvent
     * @return groupid for a given {@code iotasEvent}
     */
    protected String getGroupId(IotasEvent iotasEvent) {
        return UUID.randomUUID().toString();
    }

}
