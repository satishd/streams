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
package com.hortonworks.iotas.schemaregistry.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.iotas.schemaregistry.SchemaDto;
import com.hortonworks.iotas.schemaregistry.SchemaKey;
import com.hortonworks.iotas.schemaregistry.serde.SnapshotDeserializer;
import com.hortonworks.iotas.schemaregistry.serde.SnapshotSerializer;
import org.glassfish.jersey.client.ClientConfig;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Link;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This is the default implementation of {@link ISchemaRegistryClient} which connects to the given {@code rootCatalogURL}.
 * <pre>
 * This can be used to
 *      - register schemas
 *      - adding new versions of a schema
 *      - fetching different versions of schema
 *      - fetching latest version of a schema
 *      - check whether the given schema text is compatible with a latest version of the schema
 * </pre>
 */
public class SchemaRegistryClient implements ISchemaRegistryClient {
    private static final String SCHEMAREGISTRY_PATH = "/schemaregistry/schemas/";
    public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";

    private WebTarget webTarget;
    private Client client;

    public SchemaRegistryClient() {
    }

    @Override
    public void init(Map<String, Object> conf) {
        client = ClientBuilder.newClient(new ClientConfig());
        String rootCatalogURL = (String) conf.get(SCHEMA_REGISTRY_URL);
        webTarget = client.target(rootCatalogURL).path(SCHEMAREGISTRY_PATH);
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public SchemaKey registerSchema(Schema schema) {
        return postEntity(webTarget, new SchemaDto(schema.schemaMetadata(), schema.schemaInfo()), SchemaKey.class);
    }

    @Override
    public SchemaKey addVersionedSchema(Long schemaMetadataId, VersionedSchema schemaInfo) {
        WebTarget path = webTarget.path(schemaMetadataId.toString());
        return postEntity(path, schemaInfo, SchemaKey.class);
    }

    private <T> List<T> getEntities(WebTarget target, Class<T> clazz) {
        List<T> entities = new ArrayList<>();
        String response = target.request(MediaType.APPLICATION_JSON_TYPE).get(String.class);
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(response);
            Iterator<JsonNode> it = node.get("entities").elements();
            while (it.hasNext()) {
                entities.add(mapper.treeToValue(it.next(), clazz));
            }
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
        return entities;
    }

    private <T> T postEntity(WebTarget target, Object json, Class<T> clazz) {
        String response = target.request(MediaType.APPLICATION_JSON_TYPE).post(Entity.json(json), String.class);

        return readEntity(clazz, response);
    }

    private <T> T readEntity(Class<T> clazz, String response) {
        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(response);
            return mapper.treeToValue(node.get("entity"), clazz);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private <T> T getEntity(WebTarget target, Class<T> clazz) {
        String response = target.request(MediaType.APPLICATION_JSON_TYPE).get(String.class);

        return readEntity(clazz, response);
    }

    @Override
    public Iterable<SchemaDto> listAllSchemas() {
        return getEntities(webTarget, SchemaDto.class);
    }

    @Override
    public SchemaDto getSchema(SchemaKey schemaKey) {
        return getEntity(webTarget.path(String.format("%d/versions/%d", schemaKey.getId(), schemaKey.getVersion())), SchemaDto.class);
    }

    @Override
    public SchemaDto getLatestSchema(Long schemaMetadataId) {
        return getEntity(webTarget.path(String.format("%d/versions/latest", schemaMetadataId)), SchemaDto.class);
    }

    @Override
    public Iterable<SchemaDto> getAllVersions(Long schemaMetadataId) {
        return getEntities(webTarget.path(schemaMetadataId.toString()), SchemaDto.class);
    }

    @Override
    public boolean isCompatibleWithLatestSchema(Long schemaMetadataId, String toSchemaText) {
        WebTarget target = webTarget.path(String.format("compatibility/%d/versions/latest", schemaMetadataId));
        String response = target.request().post(Entity.text(toSchemaText), String.class);
        return readEntity(Boolean.class, response);
    }

    @Override
    public <I, O> SnapshotSerializer<I, O, Schema> getSnapshotSerializer(Long schemaMetadataId) {
        return null;
    }

    @Override
    public <O> SnapshotDeserializer<O, Schema> getSnapshotDeserializer(Long schemaMetadataId) {
        return null;
    }


}
