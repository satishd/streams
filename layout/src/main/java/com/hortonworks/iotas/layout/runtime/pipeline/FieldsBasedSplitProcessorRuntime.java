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
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.layout.design.component.Stream;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */
public class FieldsBasedSplitProcessorRuntime extends SplitProcessorRuntime {
    private final List<Schema.Field> groupFields;

    public FieldsBasedSplitProcessorRuntime(List<Schema.Field> groupFields, List<Stream> ouputStreams) {
        super(ouputStreams);
        this.groupFields = groupFields;
    }

    @Override
    public List<Result> splitEvent(IotasEvent iotasEvent) {
        List<Result> results = new ArrayList<>();
        for (Stream stream : outputStreams) {
            // todo check whether this is really needed as this logic can be pushed to stream grouping on output streams.
        }
        return null;
    }
}
