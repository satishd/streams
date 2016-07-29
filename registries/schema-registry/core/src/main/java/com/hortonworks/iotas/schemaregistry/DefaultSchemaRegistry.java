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
import com.hortonworks.iotas.common.util.FileStorage;
import com.hortonworks.iotas.storage.StorageManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class DefaultSchemaRegistry implements ISchemaRegistry {
    private static Logger LOG = LoggerFactory.getLogger(DefaultSchemaRegistry.class);

    private final StorageManager storageManager;
    private final FileStorage fileStorage;
    private final Collection<? extends SchemaProvider> schemaProviders;
    private final Map<String, SchemaProvider> schemaTypeWithProviders = new HashMap<>();
    private final Object addOrUpdateLock = new Object();

    public DefaultSchemaRegistry(StorageManager storageManager, FileStorage fileStorage, Collection<? extends SchemaProvider> schemaProviders) {
        this.storageManager = storageManager;
        this.fileStorage = fileStorage;
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
    public SchemaMetadata addSchemaMetadata(SchemaMetadata schemaMetadata) {
        final Long nextId = storageManager.nextId(schemaMetadata.getNameSpace());
        SchemaMetadata updatedSchemaMetadata = new SchemaMetadata(schemaMetadata) {{
            id = nextId;
            timestamp = System.currentTimeMillis();
        }};
        storageManager.addOrUpdate(updatedSchemaMetadata);
        return updatedSchemaMetadata;
    }

    @Override
    public SchemaMetadata getSchemaMetadata(final Long schemaMetadataId) {
        SchemaMetadata schemaMetadata = new SchemaMetadata() {{
            id = schemaMetadataId;
        }};
        return storageManager.get(schemaMetadata.getStorableKey());
    }

    @Override
    public SchemaInfo addSchemaInfo(SchemaInfo givenSchemaInfo) {
        Long schemaMetadataId = givenSchemaInfo.getSchemaMetadataId();

        Preconditions.checkNotNull(schemaMetadataId, "schemaMetadataId must not be null");

        Long id = givenSchemaInfo.getId();
        if (id != null) {
            LOG.info("Received ID [{}] in SchemaInfo instance is ignored", id);
        }
        final Long nextId = storageManager.nextId(givenSchemaInfo.getNameSpace());
        SchemaInfo schemaInfo = new SchemaInfo(givenSchemaInfo) {{
            id = nextId;
            timestamp = System.currentTimeMillis();
        }};

        //todo fix this by generating version sequence for each schema in storage layer or explore other ways to make it scalable
        synchronized (addOrUpdateLock) {
            Collection<SchemaInfo> schemaInfos = findAllVersions(schemaMetadataId);
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

    public SchemaInfo getSchemaInfo(final Long schemaInfoId) {
        SchemaInfo schemaInfo = new SchemaInfo() {{
            id = schemaInfoId;
        }};

        return storageManager.get(schemaInfo.getStorableKey());
    }

    @Override
    public Collection<SchemaInfo> listAll() {
        return storageManager.list(SchemaInfo.NAME_SPACE);
    }

    @Override
    public SchemaInfo getSchemaInfo(String type, String name, Integer version) {
        SchemaMetadata schemaMetadata = findSchemaMetadata(type, name);

        List<QueryParam> schemaInfoQueryParams =
                Lists.newArrayList(new QueryParam(SchemaInfo.SCHEMA_METADATA_ID, schemaMetadata.getId().toString()),
                        new QueryParam(SchemaInfo.VERSION, version.toString()));
        Collection<SchemaInfo> schemaInfos = storageManager.find(SchemaInfo.NAME_SPACE, schemaInfoQueryParams);

        SchemaInfo result = null;
        if (schemaInfos != null && !schemaInfos.isEmpty()) {
            if (schemaInfos.size() > 1) {
                LOG.warn("Exists more than one schema with metadataId: [{}] and version [{}]", schemaMetadata.getId(), version);
            } else {
                result = schemaInfos.iterator().next();
            }
        }
        return result;
    }

    @Override
    public Collection<SchemaInfo> findAllVersions(final Long schemaMetadataId) {
        SchemaMetadata schemaMetadata = storageManager.get(new SchemaMetadata() {{
            id = schemaMetadataId;
        }}.getStorableKey());

        Collection<SchemaInfo> result = null;
        if (schemaMetadata == null) {
            result = Collections.emptyList();
        } else {
            List<QueryParam> queryParams =
                    Collections.singletonList(new QueryParam(SchemaInfo.SCHEMA_METADATA_ID,
                            schemaMetadata.getId().toString()));
            result = storageManager.find(SchemaInfo.NAME_SPACE, queryParams);
        }

        return result;
    }

    @Override
    public SchemaInfo getSchemaInfo(final Long schemaMetadataId, Integer version) {
        SchemaMetadata schemaMetadata = storageManager.get(new SchemaMetadata() {{
            id = schemaMetadataId;
        }}.getStorableKey());

        SchemaInfo result = null;
        if (schemaMetadata != null) {
            List<QueryParam> queryParams = new ArrayList<>();
            queryParams.add(new QueryParam(SchemaInfo.SCHEMA_METADATA_ID,
                    schemaMetadata.getId().toString()));
            queryParams.add(new QueryParam(SchemaInfo.VERSION, version.toString()));
            Collection<SchemaInfo> versionedSchemas = storageManager.find(SchemaInfo.NAME_SPACE, queryParams);
            if (versionedSchemas != null && !versionedSchemas.isEmpty()) {
                LOG.warn("Exists more than one schema with metadataId: [{}] and version [{}]", schemaMetadataId, version);
                result = versionedSchemas.iterator().next();
            }
        }

        return result;
    }

    private SchemaMetadata findSchemaMetadata(String type, String name) {
        List<QueryParam> queryParams =
                Lists.newArrayList(new QueryParam(SchemaMetadata.TYPE, type),
                        new QueryParam(SchemaMetadata.NAME, name));

        Collection<SchemaMetadata> schemaInfos = storageManager.find(SchemaMetadata.NAME_SPACE, queryParams);
        SchemaMetadata schemaInfo = null;
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
    public SchemaInfo getLatestSchemaInfo(Long schemaMetadataId) {
        Collection<SchemaInfo> schemaInfos = findAllVersions(schemaMetadataId);

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

    public SchemaInfo removeSchemaInfo(final Long schemaInfoId) {
        SchemaInfo schemaInfo = new SchemaInfo() {{
            id = schemaInfoId;
        }};
        return storageManager.remove(schemaInfo.getStorableKey());
    }

    public boolean isCompatible(Long schemaMetadataId,
                                Integer version,
                                Integer toSchemaVersion) throws SchemaNotFoundException {
        SchemaInfo toSchema = getSchemaInfo(schemaMetadataId, toSchemaVersion);
        return isCompatible(schemaMetadataId, version, toSchema.getSchemaText());
    }

    public boolean isCompatible(Long schemaMetadataId,
                                Integer version,
                                String toSchema) throws SchemaNotFoundException {
        SchemaInfo existingSchemaInfo = getSchemaInfo(schemaMetadataId, version);
        String schemaText = existingSchemaInfo.getSchemaText();
        SchemaProvider.Compatibility compatibility = existingSchemaInfo.getCompatibility();
        String type = getSchemaMetadata(schemaMetadataId).getType();
        return isCompatible(type, toSchema, schemaText, compatibility);
    }

    public boolean isCompatible(String type,
                                String toSchema,
                                String existingSchema,
                                SchemaProvider.Compatibility compatibility) {
        SchemaProvider schemaProvider = schemaTypeWithProviders.get(type);
        if (schemaProvider == null) {
            throw new IllegalStateException("No SchemaProvider registered for type: " + type);
        }

        return schemaProvider.isCompatible(toSchema, existingSchema, compatibility);
    }

    public Collection<SchemaInfo> getCompatibleSchemas(Long schemaMetadataId,
                                                       SchemaProvider.Compatibility compatibility,
                                                       String toSchema)
            throws SchemaNotFoundException {
        String type = getSchemaMetadata(schemaMetadataId).getType();
        SchemaProvider schemaProvider = schemaTypeWithProviders.get(type);
        List<SchemaInfo> supportedSchemas = new ArrayList<>();
        Collection<SchemaInfo> schemaInfos = findAllVersions(schemaMetadataId);
        for (SchemaInfo schemaInfo : schemaInfos) {
            if (schemaProvider.isCompatible(toSchema, schemaInfo.getSchemaText(), compatibility)) {
                supportedSchemas.add(schemaInfo);
            }
        }

        return supportedSchemas;
    }

    @Override
    public SchemaMetadata getOrCreateSchemaMetadata(SchemaMetadata givenSchemaMetadata) {
        List<QueryParam> queryParams = new ArrayList<>();
        String name = givenSchemaMetadata.getName();
        String type = givenSchemaMetadata.getType();
        if (name != null) {
            queryParams.add(new QueryParam(SchemaMetadata.NAME, name));
        }
        if (type != null) {
            queryParams.add(new QueryParam(SchemaMetadata.TYPE, type));
        }
        SchemaMetadata schemaMetadata = null;
        Collection<SchemaMetadata> schemaMetadatas = storageManager.find(SchemaMetadata.NAME_SPACE, queryParams);
        if (schemaMetadatas == null || schemaMetadatas.isEmpty()) {
            schemaMetadata = addSchemaMetadata(givenSchemaMetadata);
        } else {
            if (schemaMetadatas.size() > 1) {
                LOG.warn("SchemaMetadata instances with name: [{}] and type: [{}] are more than one.", name, type);
            }
            schemaMetadata = schemaMetadatas.iterator().next();
        }

        return schemaMetadata;
    }

    @Override
    public Long addSerializer(SchemaSerializerInfo schemaSerializerInfo, InputStream inputStream) {
        Long nextId = storageManager.nextId(schemaSerializerInfo.getNameSpace());
        schemaSerializerInfo.setId(nextId);
        String name = getFileName(schemaSerializerInfo);
        try {
            String fileId = fileStorage.uploadFile(inputStream, name);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        storageManager.add(schemaSerializerInfo);

        return nextId;
    }

    @Override
    public SchemaSerializerInfo getSerializer(Long serializerId) {
        SchemaSerializerInfo schemaSerializerInfo = new SchemaSerializerInfo();
        schemaSerializerInfo.setId(serializerId);
        return storageManager.get(schemaSerializerInfo.getStorableKey());
    }

    @Override
    public Iterable<SchemaSerializerInfo> getSchemaSerializers(Long schemaMetadataId) {
        List<QueryParam> queryParams = Collections.singletonList(new QueryParam(SchemaSerializerInfo.SCHEMA_METADATA_ID, schemaMetadataId.toString()));
        return storageManager.find(SchemaSerializerInfo.NAME_SPACE, queryParams);
    }

    @Override
    public InputStream downloadSerializer(Long schemaMetadataId, Long serializerId) {
        SchemaSerializerInfo schemaSerializerInfo = getSerializer(serializerId);
        try {
            return fileStorage.downloadFile(getFileName(schemaSerializerInfo));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String getFileName(SchemaSerializerInfo schemaSerializerInfo) {
        return schemaSerializerInfo.getName()+"-"+schemaSerializerInfo.getId();
    }

}