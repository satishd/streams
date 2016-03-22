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
import com.hortonworks.iotas.common.IotasEventImpl;

/**
 *
 */
public class PartitionedEvent extends IotasEventImpl {

    protected String groupId;
    protected int partNo;

    public PartitionedEvent(IotasEvent iotasEvent, String streamId, String groupId, int partNo) {
        super(iotasEvent.getFieldsAndValues(), iotasEvent.getDataSourceId(), iotasEvent.getId(), iotasEvent.getHeader(), streamId, iotasEvent.getAuxiliaryFieldsAndValues());
        this.groupId = groupId;
        this.partNo = partNo;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PartitionedEvent)) return false;
        if (!super.equals(o)) return false;

        PartitionedEvent that = (PartitionedEvent) o;

        if (partNo != that.partNo) return false;
        return !(groupId != null ? !groupId.equals(that.groupId) : that.groupId != null);

    }

    @Override
    public int hashCode() {
        return partNo;
    }
}
