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
import com.hortonworks.iotas.schemaregistry.SchemaDto;
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
public class AvroSchemaRegistryTest {

    private DefaultSchemaRegistry schemaRegistry;

    @Before
    public void setup() throws IOException {
        schema1 = getSchema("/device.avsc");
        schema2 = getSchema("/device2.avsc");
        schemaName = "schema-" + System.currentTimeMillis();
        StorageManager storageManager = new InMemoryStorageManager();
        schemaRegistry = new DefaultSchemaRegistry(storageManager, null, Collections.singleton(new AvroSchemaProvider()));
        schemaRegistry.init(Collections.<String, Object>emptyMap());
    }

    protected String schema1;
    protected String schema2;
    protected String schemaName;

    private String getSchema(String schemaFileName) throws IOException {
        InputStream avroSchemaStream = AvroSerDeTest.class.getResourceAsStream(schemaFileName);
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(avroSchemaStream).toString();
    }

    @Test
    public void testRegistryOps() throws Exception {

        SchemaDto schemaDto = new SchemaDto();
        schemaDto.setSchemaText(schema1);
        schemaDto.setType(AvroSchemaProvider.TYPE);
        schemaDto.setCompatibility(SchemaProvider.Compatibility.BOTH);
        SchemaDto schemaDto1 = SchemaDto.addSchemaDto(schemaRegistry, schemaDto);
        int v1 = schemaDto1.getVersion();

        SchemaInfo schemaInfo2 = new SchemaInfo();
        schemaInfo2.setSchemaMetadataId(schemaDto1.getId());
        schemaInfo2.setCompatibility(SchemaProvider.Compatibility.BOTH);
        schemaInfo2.setSchemaText(schema2);
        SchemaInfo addedSchemaInfo2 = addSchemaAndVerify(schemaInfo2);
        int v2 = addedSchemaInfo2.getVersion();

        Assert.assertTrue(v2 == v1 + 1);

        SchemaInfo latest = schemaRegistry.getLatestSchemaInfo(schemaDto1.getId());
        Assert.assertEquals(latest, addedSchemaInfo2);

    }

    private SchemaInfo addSchemaAndVerify(SchemaInfo schemaInfo) {
        SchemaInfo addedSchemaInfo = schemaRegistry.addSchemaInfo(schemaInfo);
        Assert.assertEquals(addedSchemaInfo.getSchemaMetadataId(), schemaInfo.getSchemaMetadataId());
        Assert.assertEquals(addedSchemaInfo.getSchemaText(), schemaInfo.getSchemaText());
        return addedSchemaInfo;
    }

}