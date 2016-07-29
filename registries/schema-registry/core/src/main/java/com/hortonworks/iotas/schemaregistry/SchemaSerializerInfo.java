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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.storage.PrimaryKey;
import com.hortonworks.iotas.storage.catalog.AbstractStorable;

import java.util.Collections;

/**
 *
 */
public class SchemaSerializerInfo extends AbstractStorable {
    public static final String NAME_SPACE = "schema_serializer_info";
    public static final String ID = "id";
    public static final String SCHEMA_METADATA_ID = "schemaMetadataId";
    public static final String DESCRIPTION = "description";
    public static final String NAME = "schemaText";
    public static final String TIMESTAMP = "timestamp";

    /**
     * Unique ID generated for this component.
     */
    protected Long id;

    /**
     * Id of the SchemaMetadata instance.
     */
    private Long schemaMetadataId;

    /**
     * Description about this serializer instance
     */
    private String description;

    /**
     * Name of the serializer
     */
    private String name;

    @JsonIgnore
    @Override
    public String getNameSpace() {
        return NAME_SPACE;
    }

    @Override
    @JsonIgnore
    public PrimaryKey getPrimaryKey() {
        return new PrimaryKey(Collections.singletonMap(new Schema.Field(ID, Schema.Type.LONG), (Object) id));
    }

    @Override
    @JsonIgnore
    public Schema getSchema() {
        return Schema.of(
                Schema.Field.of(ID, Schema.Type.LONG),
                Schema.Field.of(NAME, Schema.Type.STRING),
                Schema.Field.of(SCHEMA_METADATA_ID, Schema.Type.LONG),
                Schema.Field.of(DESCRIPTION, Schema.Type.STRING),
                Schema.Field.of(TIMESTAMP, Schema.Type.LONG)
        );
    }

    @Override
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getSchemaMetadataId() {
        return schemaMetadataId;
    }

    public void setSchemaMetadataId(Long schemaMetadataId) {
        this.schemaMetadataId = schemaMetadataId;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }
}
