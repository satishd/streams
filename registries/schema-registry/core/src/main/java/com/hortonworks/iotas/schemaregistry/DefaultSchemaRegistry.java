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

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.hortonworks.iotas.common.QueryParam;
import com.hortonworks.iotas.storage.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class DefaultSchemaRegistry implements ISchemaRegistry {
    private static Logger LOG = LoggerFactory.getLogger(DefaultSchemaRegistry.class);

    private final StorageManager storageManager;
    private final Collection<? extends SchemaProvider> schemaProviders;
    private final Map<String, SchemaProvider> schemaTypeWithProviders = new HashMap<>();
    private final Object addOrUpdateLock = new Object();

    public DefaultSchemaRegistry(StorageManager storageManager, Collection<? extends SchemaProvider> schemaProviders) {
        this.storageManager = storageManager;
        this.schemaProviders = schemaProviders;
    }

    @Override
    public void init(Map<String, Object> props) {
        // initialize storage manager
        storageManager.init(props);
        for (SchemaProvider schemaProvider : schemaProviders) {
            schemaTypeWithProviders.put(schemaProvider.getType(), schemaProvider);
        }
    }

    @Override
    public SchemaInfo add(SchemaInfo schemaInfo) {
        Preconditions.checkNotNull(schemaInfo.getType(), "type must not be null");
        Preconditions.checkNotNull(schemaInfo.getName(), "name must not be null");

        Long id = schemaInfo.getId();
        if (id != null) {
            LOG.info("Received ID [{}] in SchemaInfo instance is ignored", id);
        }
        id = storageManager.nextId(schemaInfo.getNameSpace());
        schemaInfo.setId(id);
        schemaInfo.setTimestamp(System.currentTimeMillis());

        //todo fix this by generating version sequence for each schema in storage layer or explore other ways to make it scalable
        synchronized (addOrUpdateLock) {
            Collection<SchemaInfo> schemaInfos = get(schemaInfo.getType(), schemaInfo.getName());
            Integer version = 0;
            if (schemaInfos != null && !schemaInfos.isEmpty()) {
                for (SchemaInfo schema : schemaInfos) {
                    version = Math.max(schema.getVersion(), version);
                }
            }
            schemaInfo.setVersion(version + 1);
            storageManager.add(schemaInfo);
        }

        return schemaInfo;
    }

    public SchemaInfo get(Long id) {
        SchemaInfo schemaInfo = new SchemaInfo();
        schemaInfo.setId(id);

        return storageManager.get(schemaInfo.getStorableKey());
    }

    @Override
    public Collection<SchemaInfo> list() {
        return storageManager.list(SchemaInfo.NAME_SPACE);
    }

    @Override
    public SchemaInfo get(String type, String name, Integer version) {
        List<QueryParam> queryParams =
                Lists.newArrayList(new QueryParam(SchemaInfo.TYPE, type),
                        new QueryParam(SchemaInfo.NAME, name),
                        new QueryParam(SchemaInfo.VERSION, version.toString()));
        return findSchema(queryParams);
    }

    @Override
    public Collection<SchemaInfo> get(String type, String name) {
        return storageManager.find(SchemaInfo.NAME_SPACE, Lists.newArrayList(new QueryParam(SchemaInfo.NAME, name)));
    }

    private SchemaInfo findSchema(List<QueryParam> queryParams) {
        Collection<SchemaInfo> schemaInfos = storageManager.find(SchemaInfo.NAME_SPACE, queryParams);
        SchemaInfo schemaInfo = null;
        if (schemaInfos != null && !schemaInfos.isEmpty()) {
            if (schemaInfos.size() > 1) {
                LOG.warn("Received more than one schema with query parameters [{}]", queryParams);
            }
            schemaInfo = schemaInfos.iterator().next();
            LOG.debug("Schema found in registry with query parameters [{}]", queryParams);
        } else {
            LOG.debug("No schemas found in registry with query parameters [{}]", queryParams);
        }
        return schemaInfo;
    }

    @Override
    public SchemaInfo getLatest(String type, String name) {
        Collection<SchemaInfo> schemaInfos = get(type, name);

        SchemaInfo latestSchema = null;
        if (schemaInfos != null && !schemaInfos.isEmpty()) {
            Integer curVersion = Integer.MIN_VALUE;
            for (SchemaInfo schemaInfo : schemaInfos) {
                if (schemaInfo.getVersion() > curVersion) {
                    latestSchema = schemaInfo;
                    curVersion = schemaInfo.getVersion();
                }
            }
        }

        return latestSchema;
    }

    @Override
    public SchemaInfo remove(String type, String name, Integer version) {
        return storageManager.remove(get(type, name, version).getStorableKey());
    }

    public SchemaInfo remove(Long id) {
        SchemaInfo schemaInfo = new SchemaInfo();
        schemaInfo.setId(id);
        return storageManager.remove(schemaInfo.getStorableKey());
    }

    @Override
    public Collection<SchemaInfo> removeAll(String type, String name) {
        Collection<SchemaInfo> schemaInfos = get(type, name);
        for (SchemaInfo schemaInfo : schemaInfos) {
            storageManager.remove(schemaInfo.getStorableKey());
        }

        return schemaInfos;
    }

    public boolean isCompatible(String type, String name, Integer version, Integer toSchemaVersion) {
        SchemaInfo toSchema = get(type, name, toSchemaVersion);
        return isCompatible(type, name, version, toSchema.getSchemaText());
    }

    public boolean isCompatible(String type, String name, Integer version, String toSchema) {
        SchemaInfo existingSchemaInfo = get(type, name, version);
        String schemaText = existingSchemaInfo.getSchemaText();
        SchemaProvider.Compatibility compatibility = existingSchemaInfo.getCompatibility();
        return isCompatible(type, toSchema, schemaText, compatibility);
    }

    public boolean isCompatible(String type, String toSchema, String existingSchema, SchemaProvider.Compatibility compatibility) {
        SchemaProvider schemaProvider = schemaTypeWithProviders.get(type);
        if (schemaProvider == null) {
            throw new IllegalStateException("No SchemaProvider registered for type: " + type);
        }

        return schemaProvider.isCompatible(toSchema, existingSchema, compatibility);
    }

    public Collection<SchemaInfo> getCompatibleSchemas(String type, String name, SchemaProvider.Compatibility compatibility, String toSchema) {
        SchemaProvider schemaProvider = schemaTypeWithProviders.get(type);
        List<SchemaInfo> supportedSchemas = new ArrayList<>();
        Collection<SchemaInfo> schemaInfos = get(type, name);
        for (SchemaInfo schemaInfo : schemaInfos) {
            if (schemaProvider.isCompatible(toSchema, schemaInfo.getSchemaText(), compatibility)) {
                supportedSchemas.add(schemaInfo);
            }
        }

        return supportedSchemas;
    }

}