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

import java.io.InputStream;
import java.util.Collection;
import java.util.Map;

/**
 *
 */
public interface ISchemaRegistry {

    public void init(Map<String, Object> props);

    public SchemaMetadata addSchemaMetadata(SchemaMetadata schemaMetadata);

    public SchemaMetadata getSchemaMetadata(Long schemaMetadataId);

    public Collection<SchemaInfo> findAllVersions(Long schemaMetadataId);

    public SchemaInfo getSchemaInfo(Long schemaMetadataId, Integer version);

    public SchemaInfo getLatestSchemaInfo(Long schemaMetadataId);

    public SchemaInfo addSchemaInfo(SchemaInfo schemaInfo);

    public Collection<SchemaInfo> listAll();

    public SchemaInfo getSchemaInfo(String type, String name, Integer version);

    public SchemaInfo getSchemaInfo(Long schemaInfoId);

    public SchemaInfo removeSchemaInfo(Long schemaInfoId);

    public boolean isCompatible(Long schemaMetadataId, Integer existingSchemaVersion, Integer toSchemaVersion) throws SchemaNotFoundException;

    public boolean isCompatible(Long schemaMetadataId, Integer existingSchemaVersion, String schema) throws SchemaNotFoundException;

    public boolean isCompatible(String type, String toSchema, String existingSchema, SchemaProvider.Compatibility compatibility);

    public Collection<SchemaInfo> getCompatibleSchemas(Long schemaMetadataId, SchemaProvider.Compatibility compatibility, String toSchema) throws SchemaNotFoundException;

    public SchemaMetadata getOrCreateSchemaMetadata(SchemaMetadata givenSchemaMetadata);

    public Long addSerializer(SchemaSerializerInfo schemaSerializerInfo, InputStream inputStream);

    public SchemaSerializerInfo getSerializer(Long serializerId);

    public Iterable<SchemaSerializerInfo> getSchemaSerializers(Long schemaMetadataId);

    public InputStream downloadSerializer(Long schemaMetadataId, Long serializerId);
}