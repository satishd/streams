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

import com.hortonworks.iotas.common.test.IntegrationTest;
import com.hortonworks.iotas.schemaregistry.SchemaDto;
import com.hortonworks.iotas.schemaregistry.SchemaKey;
import com.hortonworks.iotas.schemaregistry.SchemaProvider;
import com.hortonworks.iotas.schemaregistry.SchemaRegistryApplication;
import com.hortonworks.iotas.schemaregistry.SchemaRegistryConfiguration;
import com.hortonworks.iotas.schemaregistry.SerializerInfo;
import com.hortonworks.iotas.schemaregistry.client.SchemaMetadata;
import com.hortonworks.iotas.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.iotas.schemaregistry.client.VersionedSchema;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
@Category(IntegrationTest.class)
public class AvroSchemaRegistryClientTest {

    @ClassRule
    public static final DropwizardAppRule<SchemaRegistryConfiguration> RULE
            = new DropwizardAppRule<>(SchemaRegistryApplication.class, ResourceHelpers.resourceFilePath("schema-registry-test.yaml"));

    private String rootUrl = String.format("http://localhost:%d/api/v1/catalog", RULE.getLocalPort());
    private SchemaRegistryClient schemaRegistryClient;

    protected String schema1;
    protected String schema2;
    protected String schemaName;

    @Before
    public void setup() throws IOException {
        schemaRegistryClient = new SchemaRegistryClient();
        schemaRegistryClient.init(Collections.singletonMap(SchemaRegistryClient.SCHEMA_REGISTRY_URL, (Object) rootUrl));
        schema1 = getSchema("/device.avsc");
        schema2 = getSchema("/device2.avsc");
        schemaName = "schema-" + System.currentTimeMillis();

    }

    private String getSchema(String schemaFileName) throws IOException {
        InputStream avroSchemaStream = AvroSerDeTest.class.getResourceAsStream(schemaFileName);
        org.apache.avro.Schema.Parser parser = new org.apache.avro.Schema.Parser();
        return parser.parse(avroSchemaStream).toString();
    }

    @Test
    public void testSchemaRelatedOps() throws Exception {

        // registering new schema-metadata
        SchemaMetadata schemaMetadata = new SchemaMetadata();
        schemaMetadata.setName("com.hwx.iot.device.schema");
        schemaMetadata.setSchemaText(schema1);
        schemaMetadata.setType(type());
        schemaMetadata.setCompatibility(SchemaProvider.Compatibility.BOTH);
        SchemaKey schemaKey1 = schemaRegistryClient.registerSchema(schemaMetadata);
        int v1 = schemaKey1.getVersion();

        // adding a new version of the schema
        VersionedSchema schemaInfo2 = new VersionedSchema();
        schemaInfo2.setSchemaText(schema2);
        SchemaKey schemaKey2 = schemaRegistryClient.addVersionedSchema(schemaKey1.getId(), schemaInfo2);
        int v2 = schemaKey2.getVersion();

        Assert.assertTrue(v2 == v1 + 1);

        SchemaDto schemaDto2 = schemaRegistryClient.getSchema(schemaKey2);
        SchemaDto latest = schemaRegistryClient.getLatestSchema(schemaKey1.getId());
        Assert.assertEquals(latest, schemaDto2);

    }

    public void testSerializerOps() throws Exception {

        // upload a jar containing serializer and deserializer classes.
        InputStream inputStream = new FileInputStream("/schema-custom-ser-des.jar");
        String fileId = schemaRegistryClient.uploadFile(inputStream);

        Long schemaMetadataId = 0L;

        // add serializer with the respective uploaded jar file id.
        SerializerInfo serializerInfo = new SerializerInfo();
        serializerInfo.setName("avro serializer");
        serializerInfo.setDescription("avro serializer");
        serializerInfo.setFileId(fileId);
        serializerInfo.setClassName("con.hwx.iotas.serializer.AvroSnapshotSerializer");
        Long serializerId = schemaRegistryClient.addSerializer(serializerInfo);

        // map this serializer with a registered schema
        schemaRegistryClient.mapSerializer(schemaMetadataId, serializerId);

        // get registered serializers
        Collection<SerializerInfo> serializers = schemaRegistryClient.getSerializers(schemaMetadataId);
        SchemaMetadata schemaMetadata = null;
        Object input = null;

        SerializerInfo registeredSerializerInfo = serializers.iterator().next();

        //get serializer and serialize the given payload
        try(AvroSnapshotSerializer snapshotSerializer = schemaRegistryClient.createInstance(registeredSerializerInfo);) {
            Map<String, Object> config = Collections.emptyMap();
            snapshotSerializer.init(config);

            byte[] serializedData = snapshotSerializer.serialize(input, schemaMetadata);
        }

    }

    private String type() {
        return AvroSchemaProvider.TYPE;
    }

}
