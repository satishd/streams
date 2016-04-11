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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Runtime for {@link SplitAction}
 */
public class SplitActionRuntime implements ActionRuntime {

    private static final Logger log = LoggerFactory.getLogger(SplitActionRuntime.class);

    public static final String SPLIT_GROUP_ID = "com.hortonworks.iotas.split.group_id";
    public static final String SPLIT_PARTITION_ID = "com.hortonworks.iotas.split.partition_id";
    public static final String SPLIT_TOTAL_PARTITIONS_ID = "com.hortonworks.iotas.split.partition.total.count";

    private final SplitAction splitAction;
    private Splitter splitter = null;

    public SplitActionRuntime(SplitAction splitAction) {
        this.splitAction = splitAction;
    }

    public void prepare() {
        final String jarId = splitAction.getJarId();
        final String splitterClassName = splitAction.getSplitterClassName();
        ProxyUtil<Splitter> proxyUtil = new ProxyUtil<>(Splitter.class, this.getClass().getClassLoader());
        try {
            splitter = proxyUtil.loadClassFromJar(jarId, splitterClassName);
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        }
    }

    @Override
    public List<Result> execute(IotasEvent input) {
        // based on split-action configuration, generate events for respective streams
        final List<Result> results = splitter.splitEvent(input);
        //todo add groupId and no of partitions etc as part of headers before they are sent from here.
        // above should be done by splitEvent method and this action does not add any of those values as they are
        // specific to the split logic.

        // check whether the split event has all the required split info.
        for (Result result : results) {
            for (IotasEvent event : result.events) {
                checkGroupIdPartitionId(event);
            }
        }

        return results;
    }

    private void checkGroupIdPartitionId(IotasEvent event) {
        final Map<String, Object> header = event.getHeader();

        if(header == null) {
            log.error("Event [{}] does not have headers", event);
            throw new IllegalStateException("header can not be null for split events");
        }
        if(header.get(SPLIT_GROUP_ID) == null || header.get(SPLIT_PARTITION_ID) == null) {
            log.error("Event [{}] does not have complete split event info with group-id:[{}] and partition-id:[{}]", event, header.get(SPLIT_GROUP_ID), header.get(SPLIT_PARTITION_ID));
            throw new IllegalStateException("header should have group-id and partition-id for split events");
        }

    }

    @Override
    public List<String> getOutputStreams() {
        return splitAction.getOutputStreams();
    }
}
