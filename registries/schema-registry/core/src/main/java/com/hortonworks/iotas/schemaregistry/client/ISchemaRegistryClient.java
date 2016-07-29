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

import com.hortonworks.iotas.schemaregistry.SchemaDto;
import com.hortonworks.iotas.schemaregistry.SchemaKey;
import com.hortonworks.iotas.schemaregistry.serde.SnapshotDeserializer;
import com.hortonworks.iotas.schemaregistry.serde.SnapshotSerializer;

import java.util.Map;

/**
 * This interface defines different ways to register and retrieve schemas from remote schema registry.
 */
public interface ISchemaRegistryClient {

    /**
     * Initializes the client with the configuration.
     *
     * @param conf
     */
    public void init(Map<String, Object> conf);

    /**
     * Closes any resources held by this client.
     */
    public void close();

    /**
     * Returns {@link SchemaKey} of the added or an existing schema.
     * <pre>
     * It tries to fetch an existing schema or register the given schema with the below conditions
     *  - Checks whether there exists a schema with the given schemaText, type and name
     *      - returns respective schemaKey if it exists.
     *      - Creates a schema for the given name and returns respective schemaKey if it does not exist
     * </pre>
     * @param schema
     * @return
     */
    public SchemaKey registerSchema(Schema schema);

    /**
     * Returns {@link SchemaKey} after adding the given schema as the next version of the schema.
     *
     * @param schemaMetadataId id of the schema metadata.
     * @param versionedSchema
     * @return
     */
    public SchemaKey addVersionedSchema(Long schemaMetadataId, VersionedSchema versionedSchema);

    /**
     * Returns all schemas registered in the repository. It may be paging the results internally with out realizing all
     * the results.
     *
     * @return
     */
    public Iterable<SchemaDto> listAllSchemas();

    /**
     * Returns {@link SchemaDto} for the given {@link SchemaKey}
     *
     * @param schemaKey
     * @return
     */
    public SchemaDto getSchema(SchemaKey schemaKey);

    /**
     * Returns the latest version of the schema for the given {@param schemaMetadataId}
     *
     * @param schemaMetadataId
     * @return
     */
    public SchemaDto getLatestSchema(Long schemaMetadataId);

    /**
     * Returns all versions of the schemas for given {@param schemaMetadataId}
     *
     * @param schemaMetadataId
     * @return
     */
    public Iterable<SchemaDto> getAllVersions(Long schemaMetadataId);

    /**
     * Returns true if the given {@code toSchemaText} is compatible with the latest version of the schema with id as {@code schemaMetadataId}.
     *
     * @param schemaMetadataId
     * @param toSchemaText
     * @return
     */
    public boolean isCompatibleWithLatestSchema(Long schemaMetadataId, String toSchemaText);

    public <I, O> SnapshotSerializer<I, O, Schema> getSnapshotSerializer(Long schemaMetadataId);

    public <O> SnapshotDeserializer<O, Schema> getSnapshotDeserializer(Long schemaMetadataId);

}
