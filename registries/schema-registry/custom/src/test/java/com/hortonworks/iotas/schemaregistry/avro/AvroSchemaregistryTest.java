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
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;

/**
 *
 */
public class AvroSchemaregistryTest {

    private DefaultSchemaRegistry schemaRegistry;
    private String schema1;
    private String schema2;

    @Before
    public void setup() throws IOException {
        schema1 = getSchema("/device.avsc");
        schema2 = getSchema("/device2.avsc");
        StorageManager storageManager = new InMemoryStorageManager();
        schemaRegistry = new DefaultSchemaRegistry(storageManager, Collections.singleton(new AvroSchemaProvider()));
        schemaRegistry.init(Collections.<String, Object>emptyMap());
    }

    private String getSchema(String schemaFileName) throws IOException {
        InputStream avroSchemaStream = AvroSerDeTest.class.getResourceAsStream(schemaFileName);
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(avroSchemaStream).toString();
    }

    @Test
    public void testRegistryOps() {
        String type = AvroSchemaProvider.TYPE;

        SchemaInfo schemaInfo = new SchemaInfo("schema-"+System.currentTimeMillis(), type);
        schemaInfo.setCompatibility(SchemaProvider.Compatibility.BOTH);
        schemaInfo.setSchemaText(schema1);
        SchemaInfo addedSchemaInfo = schemaRegistry.add(schemaInfo);

        schemaInfo.setVersion(1);
        schemaInfo.setId(addedSchemaInfo.getId());
        schemaInfo.setTimestamp(addedSchemaInfo.getTimestamp());
        Assert.assertEquals(addedSchemaInfo, schemaInfo);

        schemaInfo.setSchemaText(schema2);
        addedSchemaInfo = schemaRegistry.add(schemaInfo);

        schemaInfo.setVersion(2);
        schemaInfo.setId(addedSchemaInfo.getId());
        schemaInfo.setTimestamp(addedSchemaInfo.getTimestamp());
        Assert.assertEquals(addedSchemaInfo, schemaInfo);

        SchemaInfo latest = schemaRegistry.getLatest(type, schemaInfo.getName());
        Assert.assertEquals(latest, schemaInfo);

        Assert.assertTrue(schemaRegistry.isCompatible(type, schema1, schema2, SchemaProvider.Compatibility.BACKWARD));
        Assert.assertTrue(schemaRegistry.isCompatible(type, schema1, schema2, SchemaProvider.Compatibility.FORWARD));
        Assert.assertTrue(schemaRegistry.isCompatible(type, schema1, schema2, SchemaProvider.Compatibility.BOTH));
    }
}
