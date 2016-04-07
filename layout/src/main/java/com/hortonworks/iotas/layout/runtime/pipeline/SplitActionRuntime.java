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

import java.util.List;

/**
 * Runtime for {@link SplitAction}
 */
public class SplitActionRuntime implements ActionRuntime {
    private final SplitAction splitAction;

    public SplitActionRuntime(SplitAction splitAction) {
        this.splitAction = splitAction;
    }

    @Override
    public List<Result> execute(IotasEvent input) {
        // based on split-action configuration, generate events for respective streams
        final String jarId = splitAction.getJarId();
        final String splitterClassName = splitAction.getSplitterClassName();
        ProxyUtil<Splitter> proxyUtil = new ProxyUtil<>(Splitter.class, this.getClass().getClassLoader());
        Splitter splitter = null;
        try {
            splitter = proxyUtil.loadClassFromJar(jarId, splitterClassName);
            final List<Result> results = splitter.splitEvent(input);
            //todo add groupId and no of partitions etc as part of headers before they are sent from here.
            return results;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public List<String> getOutputStreams() {
        return splitAction.getOutputStreams();
    }
}
