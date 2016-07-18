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

import com.google.common.collect.Lists;
import com.hortonworks.iotas.common.QueryParam;
import com.hortonworks.iotas.storage.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class DefaultSchemaRegistry implements ISchemaRegistry {
    private static Logger LOG = LoggerFactory.getLogger(DefaultSchemaRegistry.class);

    private StorageManager storageManager;

    public DefaultSchemaRegistry(StorageManager storageManager) {
        this.storageManager = storageManager;
    }

    @Override
    public void init(Map<String, Object> props) {
        // initialize storage manager
    }

    @Override
    public Long add(SchemaInfo schemaInfo) {
        Long id = schemaInfo.getId();
        if(id == null) {
            id = storageManager.nextId(schemaInfo.getNameSpace());
            schemaInfo.setId(id);
        }
        schemaInfo.setTimestamp(System.currentTimeMillis());

        storageManager.add(schemaInfo);

        return id;
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
    public SchemaInfo get(String name, Integer version) {
        List<QueryParam> queryParams = Lists.newArrayList( new QueryParam(SchemaInfo.NAME, name),
                                                            new QueryParam(SchemaInfo.VERSION, version.toString()));
        return findSchema(queryParams);
    }


    @Override
    public Collection<SchemaInfo> get(String name) {
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
    public SchemaInfo getLatest(String name) {
        Collection<SchemaInfo> schemaInfos = get(name);

        SchemaInfo latestSchema = null;
        if(schemaInfos != null && !schemaInfos.isEmpty()) {
            Integer curVersion = Integer.MIN_VALUE;
            for (SchemaInfo schemaInfo : schemaInfos) {
                if(schemaInfo.getVersion() > curVersion) {
                    latestSchema = schemaInfo;
                    curVersion = schemaInfo.getVersion();
                }
            }
        }

        return latestSchema;
    }

    @Override
    public SchemaInfo remove(String name, Integer version) {
        return storageManager.remove(get(name, version).getStorableKey());
    }

    public SchemaInfo remove(Long id) {
        SchemaInfo schemaInfo = new SchemaInfo();
        schemaInfo.setId(id);
        return storageManager.remove(schemaInfo.getStorableKey());
    }

    @Override
    public Collection<SchemaInfo> removeAll(String name) {
        Collection<SchemaInfo> schemaInfos = get(name);
        for (SchemaInfo schemaInfo : schemaInfos) {
            storageManager.remove(schemaInfo.getStorableKey());
        }

        return schemaInfos;
    }
}
