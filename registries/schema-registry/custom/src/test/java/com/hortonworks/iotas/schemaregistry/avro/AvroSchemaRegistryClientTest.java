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
import com.hortonworks.iotas.schemaregistry.SchemaInfo;
import com.hortonworks.iotas.schemaregistry.SchemaRegistryApplication;
import com.hortonworks.iotas.schemaregistry.SchemaRegistryClient;
import com.hortonworks.iotas.schemaregistry.SchemaRegistryConfiguration;
import io.dropwizard.testing.ResourceHelpers;
import io.dropwizard.testing.junit.DropwizardAppRule;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;

import java.io.IOException;

/**
 *
 */
@Category(IntegrationTest.class)
public class AvroSchemaRegistryClientTest extends AbstractAvroSchemaRegistryTest{
    private static final String type = AvroSchemaProvider.TYPE;

    @ClassRule
    public static final DropwizardAppRule<SchemaRegistryConfiguration> RULE
            = new DropwizardAppRule<>(SchemaRegistryApplication.class, ResourceHelpers.resourceFilePath("schema-registry-test.yaml"));

    private String rootUrl = String.format("http://localhost:%d/api/v1/catalog/", RULE.getLocalPort());
    private SchemaRegistryClient schemaRegistryClient;

    @Before
    public void setup() throws IOException {
        super.setup();
        schemaRegistryClient = new SchemaRegistryClient(rootUrl);
    }

    @Override
    protected SchemaInfo getLatestSchema(String type, String name) {
        return schemaRegistryClient.getLatest(type, name);
    }

    @Override
    protected SchemaInfo addSchema(SchemaInfo schemaInfo) {
        return schemaRegistryClient.add(schemaInfo);
    }

    @Override
    protected void testCompatibility(String type, String schema1, String schema2) {

    }

}
