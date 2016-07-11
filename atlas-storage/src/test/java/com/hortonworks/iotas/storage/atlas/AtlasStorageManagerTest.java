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

import com.hortonworks.iotas.common.test.IntegrationTest;
import com.hortonworks.iotas.storage.AbstractStoreManagerTest;
import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.StorableTest;
import com.hortonworks.iotas.storage.StorageManager;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.ArrayList;
import java.util.Collections;

/**
 *
 */
@Category(IntegrationTest.class)
public class AtlasStorageManagerTest extends AbstractStoreManagerTest {
    private static AtlasStorageManager atlasStorageManager;

    @BeforeClass
    public static void setUpClass() throws Exception {
        atlasStorageManager = new AtlasStorageManager();
        atlasStorageManager.init(null);
        atlasStorageManager.registerStorables(Collections.<Class<? extends Storable>>singletonList(DeviceInfo.class));
    }

    @Override
    protected void setStorableTests() {
        storableTests = new ArrayList<>();
        storableTests.add(new StorableTest() {
                              {
                                  storableList = new ArrayList<Storable>() {
                                      {
                                          long x = System.currentTimeMillis();
                                          add(createDeviceInfo(x, "deviceinfo-" + x));
                                          add(createDeviceInfo(x, "deviceinfo-" + (x + 1)));
                                          add(createDeviceInfo(++x, "deviceinfo-" + x));
                                          add(createDeviceInfo(++x, "deviceinfo-" + x));
                                      }
                                  };
                              }

                              private Storable createDeviceInfo(long id, String name) {
                                  DeviceInfo deviceInfo = new DeviceInfo();
                                  deviceInfo.setXid(id+"");
                                  deviceInfo.setName(name);
                                  deviceInfo.setTimestamp(""+System.currentTimeMillis());
                                  deviceInfo.setVersion(""+System.nanoTime());
                                  return deviceInfo;
                              }
                          }
        );
    }

    @Override
    protected StorageManager getStorageManager() {
        return atlasStorageManager;
    }


    @Test
    @Ignore
    public void testCrud_AllStorableEntities_NoExceptions() {
        // currently Atlas does not support returning removed info for remove APIs because of which the super class's
        // implementation fails.
    }

    @Test
    public void testNextId_AutoincrementColumn_IdPlusOne() throws Exception {
        for (StorableTest storableTest : storableTests) {
            Long currentId = getStorageManager().nextId(storableTest.getNameSpace());
            for(int i=0; i<100; i++) {
                Long nextId = getStorageManager().nextId(storableTest.getNameSpace());
                Assert.assertTrue(currentId < nextId);
                currentId = nextId;
            }
        }
    }
}
