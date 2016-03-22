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
import com.hortonworks.iotas.common.Result;
import com.hortonworks.iotas.layout.design.component.IotasProcessor;
import com.hortonworks.iotas.layout.design.component.Processor;
import com.hortonworks.iotas.layout.runtime.ActionRuntime;

import java.util.List;

/**
 * Split the payload based on a given criteria. It splits the payload and sends it to parallel stages. Each stage can have
 * List of Actions. Current supported Action is Enrichment and other actions like projection/filters etc can be added.
 */
public abstract class SplitProcessor extends IotasProcessor {

    public static final String ROOT_MESSAGE_STREAM = "root-message-stream";
    protected final List<Stage> parallelStages;

    protected SplitProcessor(List<Stage> parallelStages) {
        this.parallelStages = parallelStages;
    }

    public abstract List<Result> splitPayload(IotasEvent iotasEvent);
}
