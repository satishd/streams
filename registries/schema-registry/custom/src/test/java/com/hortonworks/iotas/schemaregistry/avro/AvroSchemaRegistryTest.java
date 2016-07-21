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
package com.hortonworks.iotas.schemaregistry.avro;

import com.hortonworks.iotas.schemaregistry.DefaultSchemaRegistry;
import com.hortonworks.iotas.schemaregistry.SchemaInfo;
import com.hortonworks.iotas.schemaregistry.SchemaProvider;
import com.hortonworks.iotas.storage.StorageManager;
import com.hortonworks.iotas.storage.impl.memory.InMemoryStorageManager;
import org.junit.Assert;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

/**
 *
 */
public class AvroSchemaRegistryTest extends AbstractAvroSchemaRegistryTest {

    private DefaultSchemaRegistry schemaRegistry;

    @Before
    public void setup() throws IOException {
        super.setup();
        StorageManager storageManager = new InMemoryStorageManager();
        schemaRegistry = new DefaultSchemaRegistry(storageManager, Collections.singleton(new AvroSchemaProvider()));
        schemaRegistry.init(Collections.<String, Object>emptyMap());
    }

    protected void testCompatibility(String type, String schema1, String schema2) {
        Assert.assertTrue(schemaRegistry.isCompatible(type, schema1, schema2, SchemaProvider.Compatibility.BACKWARD));
        Assert.assertTrue(schemaRegistry.isCompatible(type, schema1, schema2, SchemaProvider.Compatibility.FORWARD));
        Assert.assertTrue(schemaRegistry.isCompatible(type, schema1, schema2, SchemaProvider.Compatibility.BOTH));
    }

    protected SchemaInfo getLatestSchema(String type, String name) {
        return schemaRegistry.getLatest(type, name);
    }

    protected SchemaInfo addSchema(SchemaInfo schemaInfo) {
        return schemaRegistry.add(schemaInfo);
    }
}
