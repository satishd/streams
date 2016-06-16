/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks.iotas.storage.atlas;

import com.google.common.base.Preconditions;
import org.apache.atlas.AtlasException;
import org.apache.atlas.typesystem.Referenceable;
import org.apache.atlas.typesystem.types.AttributeDefinition;
import org.apache.atlas.typesystem.types.ClassType;
import org.apache.atlas.typesystem.types.DataTypes;
import org.apache.atlas.typesystem.types.HierarchicalTypeDefinition;
import org.apache.atlas.typesystem.types.IDataType;
import org.apache.atlas.typesystem.types.Multiplicity;
import org.apache.atlas.typesystem.types.utils.TypesUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.Random;

/**
 *
 */
public class AtlasMetadataServiceTest {

    @Test
    public void testParserTypes() throws Exception {
        AtlasMetadataService atlasMetadataService = new AtlasMetadataService();
        atlasMetadataService.registerType(createDeviceInfoType());

        DeviceInfo deviceInfo = createDeviceInfo();

        String instanceId = atlasMetadataService.createEntity(toReferenceable(deviceInfo));
        Referenceable entity = atlasMetadataService.getEntity(instanceId);
        DeviceInfo storedDeviceInfo = fromReferenceable(entity);

        Assert.assertEquals(deviceInfo, storedDeviceInfo);

        long timestamp = System.currentTimeMillis();
        entity.set(DeviceInfo.TIMESTAMP, timestamp);
        atlasMetadataService.addOrUpdateEntity(entity);
        Assert.assertEquals(atlasMetadataService.getEntity(instanceId).get(DeviceInfo.TIMESTAMP), ""+timestamp);

        atlasMetadataService.remove(DeviceInfo.NAME_SPACE, Collections.<String, Object>singletonMap(DeviceInfo.XID, deviceInfo.getXid()));
        try {
            Referenceable deletedEntity = atlasMetadataService.getEntity(instanceId);
            Assert.fail("getEntity should have thrown an exception here.");
        } catch (AtlasException e) {
            //expecting an exception, ignore it.
        }
    }

    private Referenceable toReferenceable(DeviceInfo deviceInfo) {
        return new Referenceable(deviceInfo.getNameSpace(), deviceInfo.toMap());
    }

    public static HierarchicalTypeDefinition<ClassType> createDeviceInfoType() {
        return TypesUtil.createClassTypeDef(
                DeviceInfo.NAME_SPACE, null,
                TypesUtil.createUniqueRequiredAttrDef(DeviceInfo.NAME, DataTypes.STRING_TYPE),
                TypesUtil.createUniqueRequiredAttrDef(DeviceInfo.XID, DataTypes.LONG_TYPE),
                attrDef(DeviceInfo.TIMESTAMP, DataTypes.LONG_TYPE),
                attrDef(DeviceInfo.VERSION, DataTypes.STRING_TYPE)
        );
    }

    private static AttributeDefinition attrDef(String name, IDataType dT) {
        return attrDef(name, dT, Multiplicity.OPTIONAL, false, null);
    }

    private static AttributeDefinition attrDef(String name, IDataType dT, Multiplicity m, boolean isComposite,
                                               String reverseAttributeName) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(dT);
        return new AttributeDefinition(name, dT.getName(), m, isComposite, false, false, reverseAttributeName);
    }

    private DeviceInfo fromReferenceable(Referenceable referenceable) {
        DeviceInfo deviceInfo = new DeviceInfo();

        deviceInfo.setXid(referenceable.get(DeviceInfo.XID).toString());
        deviceInfo.setName(referenceable.get(DeviceInfo.NAME).toString());
        deviceInfo.setTimestamp(referenceable.get(DeviceInfo.TIMESTAMP).toString());
        deviceInfo.setVersion(referenceable.get(DeviceInfo.VERSION).toString());

        return deviceInfo;
    }

    protected static DeviceInfo createDeviceInfo() {
        DeviceInfo deviceInfo = new DeviceInfo();
        deviceInfo.setName("device-" + System.currentTimeMillis());
        deviceInfo.setXid(System.currentTimeMillis()+"");
        deviceInfo.setVersion(""+new Random().nextInt() % 10L);
        deviceInfo.setTimestamp(""+System.currentTimeMillis());

        return deviceInfo;
    }

    public static void main(String[] args) throws Exception {
        AtlasMetadataServiceTest atlasMetadataServiceTest = new AtlasMetadataServiceTest();
        atlasMetadataServiceTest.testParserTypes();
    }


}
