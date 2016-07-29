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

import com.hortonworks.iotas.schemaregistry.SchemaInfo;
import com.hortonworks.iotas.schemaregistry.SchemaMetadata;
import com.hortonworks.iotas.schemaregistry.SchemaProvider;

import java.io.Serializable;

/**
 * This class encapsulates schema information including name, type, description, schemaText and compatibility. This can
 * be used when client does not know about the existing registered schema information.
 *
 */
public final class Schema implements Serializable {

    // name for schema which is part of schema metadata
    private String name;

    // type for schema which is part of schema metadata, which can be AVRO, JSON, PROTOBUF etc
    private String type;

    // description of the schema which is given in schema metadata
    private String description;

    // textual representation of the schema which is given in SchemaInfo
    private String schemaText;

    // Compatibility of the schema for a given version which is given in SchemaInfo
    private SchemaProvider.Compatibility compatibility;

    public Schema() {
    }

    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getDescription() {
        return description;
    }

    public String getSchemaText() {
        return schemaText;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public void setSchemaText(String schemaText) {
        this.schemaText = schemaText;
    }

    public SchemaProvider.Compatibility getCompatibility() {
        return compatibility;
    }

    public void setCompatibility(SchemaProvider.Compatibility compatibility) {
        this.compatibility = compatibility;
    }

    public SchemaMetadata schemaMetadata() {
        SchemaMetadata schemaMetadata = new SchemaMetadata();
        schemaMetadata.setName(name);
        schemaMetadata.setType(type);
        schemaMetadata.setDescription(description);

        return schemaMetadata;
    }

    public SchemaInfo schemaInfo() {
        SchemaInfo schemaInfo = new SchemaInfo();
        schemaInfo.setSchemaText(schemaText);
        schemaInfo.setCompatibility(compatibility);

        return schemaInfo;
    }
}
