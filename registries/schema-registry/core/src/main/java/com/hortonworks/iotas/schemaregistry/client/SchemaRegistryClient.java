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
import com.hortonworks.iotas.schemaregistry.DeserializerInfo;
import com.hortonworks.iotas.schemaregistry.IncompatibleSchemaException;
import com.hortonworks.iotas.schemaregistry.InvalidSchemaException;
import com.hortonworks.iotas.schemaregistry.SchemaDto;
import com.hortonworks.iotas.schemaregistry.SchemaKey;
import com.hortonworks.iotas.schemaregistry.SerDesInfo;
import com.hortonworks.iotas.schemaregistry.SerializerInfo;
import com.hortonworks.iotas.schemaregistry.serde.SerDeException;
import org.glassfish.jersey.client.ClientConfig;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * This is the default implementation of {@link ISchemaRegistryClient} which connects to the given {@code rootCatalogURL}.
 * <pre>
 * This can be used to
 *      - register schemas
 *      - add new versions of a schema
 *      - fetch different versions of schema
 *      - fetch latest version of a schema
 *      - check whether the given schema text is compatible with a latest version of the schema
 *      - register serializer/deserializer for a schema
 *      - fetch serializer/deserializer for a schema
 * </pre>
 */
public class SchemaRegistryClient implements ISchemaRegistryClient {
    private static final String SCHEMA_REGISTRY_PATH = "/schemaregistry/schemas/";
    public static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
    public static final String JAR_STORAGE_LOCATION = "jar.storage.location";
    public static final String LOCAL_JAR_PATH = "schema.registry.local.jars.path";
    public static final String CLASSLOADER_CACHE_SIZE = "schema.registry.class.loader.cache.size";

    private WebTarget webTarget;
    private Client client;

    public SchemaRegistryClient() {
    }

    @Override
    public void init(Map<String, Object> conf) {
        client = ClientBuilder.newClient(new ClientConfig());
        String rootCatalogURL = (String) conf.get(SCHEMA_REGISTRY_URL);
        webTarget = client.target(rootCatalogURL).path(SCHEMA_REGISTRY_PATH);
    }

    @Override
    public void close() {
        client.close();
    }

    @Override
    public SchemaKey registerSchema(SchemaMetadata schemaMetadata) {
        return postEntity(webTarget, new SchemaDto(schemaMetadata.schemaMetadataStorable(), schemaMetadata.schemaInfoStorable()), SchemaKey.class);
    }

    @Override
    public SchemaKey addVersionedSchema(Long schemaMetadataId, VersionedSchema schemaInfo) throws InvalidSchemaException, IncompatibleSchemaException {
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
    public String uploadFile(InputStream inputStream) {
        return null;
    }

    @Override
    public InputStream downloadFile(String fileId) {
        return null;
    }

    @Override
    public Long addSerializer(SerializerInfo serializerInfo) {
        return null;
    }

    @Override
    public Long addDeserializer(DeserializerInfo deserializerInfo) {
        return null;
    }

    @Override
    public Collection<SerializerInfo> getSerializers(Long schemaMetadataId) {
        return null;
    }

    @Override
    public Collection<DeserializerInfo> getDeserializers(Long schemaMetadataId) {
        return null;
    }

    public void mapSerializer(Long schemaMetadataId, Long serializerId) {
        // map internally
    }

    public void mapDeserializer(Long schemaMetadataId, Long deserializerId) {
        // map internally
    }

    public <T> T createInstance(SerDesInfo serDesInfo) {
        // loading serializer, create a class loader and and keep them in cache.
        String fileId = serDesInfo.getFileId();
        // get class loader for this file ID
        ClassLoader classLoader = getClassLoader(fileId);

        //
        T t = null;
        try {
            Class<T> clazz = (Class<T>) Class.forName(serDesInfo.getClassName(), true, classLoader);
            t = clazz.newInstance();
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new SerDeException(e);
        }

        return t;
    }

    private ClassLoader getClassLoader(String fileId) {
        return null;
    }

}
