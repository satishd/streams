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

import java.util.Map;

/**
 *
 */
public class GroupRootEvent implements IotasEvent {

    private final IotasEvent iotasEvent;
    protected String groupId;
    protected int noOfMessages;
    private final String sourceStreamId;

    public GroupRootEvent(IotasEvent iotasEvent, String groupId, int noOfMessages, String sourceStreamId) {
        this.iotasEvent = iotasEvent;
        this.groupId = groupId;
        this.noOfMessages = noOfMessages;
        this.sourceStreamId = sourceStreamId;
    }

    @Override
    public Map<String, Object> getFieldsAndValues() {
        return iotasEvent.getFieldsAndValues();
    }

    @Override
    public Map<String, Object> getAuxiliaryFieldsAndValues() {
        return iotasEvent.getAuxiliaryFieldsAndValues();
    }

    @Override
    public void addAuxiliaryFieldAndValue(String field, Object value) {
        iotasEvent.addAuxiliaryFieldAndValue(field, value);
    }

    @Override
    public Map<String, Object> getHeader() {
        return iotasEvent.getHeader();
    }

    @Override
    public String getId() {
        return iotasEvent.getId();
    }

    @Override
    public String getDataSourceId() {
        return iotasEvent.getDataSourceId();
    }

    @Override
    public String getSourceStream() {
        return sourceStreamId;
    }
}
