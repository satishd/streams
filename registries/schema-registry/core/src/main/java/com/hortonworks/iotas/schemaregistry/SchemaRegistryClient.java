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
package com.hortonworks.iotas.schemaregistry;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hortonworks.iotas.storage.Storable;
import org.glassfish.jersey.client.ClientConfig;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class SchemaRegistryClient implements ISchemaRegistryClient {
    private static final String SCHEMAREGISTRY_PATH = "/schemaregistry";

    private final WebTarget webTarget;
    private final String rootCatalogURL;

    public SchemaRegistryClient(String rootCatalogURL) {
        this(rootCatalogURL, new ClientConfig());
    }

    public SchemaRegistryClient(String rootCatalogURL, ClientConfig clientConfig) {
        this.rootCatalogURL = rootCatalogURL;
        Client client = ClientBuilder.newClient(clientConfig);
        webTarget = client.target(rootCatalogURL).path(SCHEMAREGISTRY_PATH);
    }

    @Override
    public Long add(SchemaInfo schemaInfo) {
        return webTarget.request().get(Long.class);
    }

    private <T extends Storable> List<T> getEntities(WebTarget target, Class<T> clazz) {
        List<T> entities = new ArrayList<T>();
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

    private <T extends Storable> T getEntity(WebTarget target, Class<T> clazz) {
        String response = target.request(MediaType.APPLICATION_JSON_TYPE).get(String.class);

        try {
            ObjectMapper mapper = new ObjectMapper();
            JsonNode node = mapper.readTree(response);
            return mapper.treeToValue(node.get("entity"), clazz);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Collection<SchemaInfo> list() {
        return getEntities(webTarget, SchemaInfo.class);
    }

    @Override
    public SchemaInfo get(String name, Integer version) {
        return getEntity(webTarget.path(String.format("/%s/%s", name, version)), SchemaInfo.class);
    }

    @Override
    public SchemaInfo get(Long id) {
        return getEntity(webTarget.path(id+""), SchemaInfo.class);
    }

    @Override
    public SchemaInfo getLatest(String name) {
        return getEntity(webTarget.path(String.format("/%s/latest", name)), SchemaInfo.class);
    }

    @Override
    public Collection<SchemaInfo> get(String name) {
        return getEntities(webTarget.path(String.format("/%s", name)), SchemaInfo.class);
    }
}
