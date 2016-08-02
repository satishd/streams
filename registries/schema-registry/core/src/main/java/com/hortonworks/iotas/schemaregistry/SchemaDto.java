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

/**
 *
 */
public final class SchemaDto {

    // schema metadata id
    private Long id;

    // name for schema which is part of schema metadata
    private String name;

    // type for schema which is part of schema metadata, which can be AVRO, JSON, PROTOBUF etc
    private String type;

    // description of the schema which is given in schema metadata
    private String description;

    // version of the schema which is given in SchemaInfo
    private Integer version;

    // textual representation of the schema which is given in SchemaInfo
    private String schemaText;

    // timestamp of the schema which is given in SchemaInfo
    private Long timestamp;

    // Compatibility of the schema for a given version which is given in SchemaInfo
    private SchemaProvider.Compatibility compatibility;

    public SchemaDto() {
    }

    public SchemaDto(SchemaMetadataStorable schemaMetadataStorable, SchemaInfoStorable schemaInfoStorable) {
        id = schemaMetadataStorable.getId();
        name = schemaMetadataStorable.getName();
        type = schemaMetadataStorable.getType();
        description = schemaMetadataStorable.getDescription();
        version = schemaInfoStorable.getVersion();
        schemaText = schemaInfoStorable.getSchemaText();
        timestamp = schemaInfoStorable.getTimestamp();
        compatibility = schemaMetadataStorable.getCompatibility();
    }

    public Long getId() {
        return id;
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

    public Integer getVersion() {
        return version;
    }

    public String getSchemaText() {
        return schemaText;
    }

    public Long getTimestamp() {
        return timestamp;
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

    private SchemaMetadataStorable schemaMetadataStorable() {
        SchemaMetadataStorable schemaMetadataStorable = new SchemaMetadataStorable();
        schemaMetadataStorable.setName(name);
        schemaMetadataStorable.setType(type);
        schemaMetadataStorable.setDescription(description);

        return schemaMetadataStorable;
    }

    private SchemaInfoStorable schemaInfoStorable() {
        SchemaInfoStorable schemaInfoStorable = new SchemaInfoStorable(){{id=id; timestamp=timestamp;}};
        schemaInfoStorable.setVersion(version);
        schemaInfoStorable.setSchemaText(schemaText);

        return schemaInfoStorable;
    }

    public static SchemaDto addSchemaDto(ISchemaRegistry schemaRegistry, SchemaDto schemaDto) {
        SchemaMetadataStorable givenSchemaMetadataStorable = schemaDto.schemaMetadataStorable();
        SchemaMetadataStorable schemaMetadataStorable = schemaRegistry.getOrCreateSchemaMetadata(givenSchemaMetadataStorable);

        SchemaInfoStorable givenSchemaInfoStorable = schemaDto.schemaInfoStorable();
        givenSchemaInfoStorable.setSchemaMetadataId(schemaMetadataStorable.getId());
        SchemaInfoStorable addedSchemaInfoStorable = schemaRegistry.addSchemaInfo(givenSchemaInfoStorable);

        return new SchemaDto(schemaMetadataStorable, addedSchemaInfoStorable);
    }


    @Override
    public String toString() {
        return "SchemaDto{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", type='" + type + '\'' +
                ", description='" + description + '\'' +
                ", version=" + version +
                ", schemaText='" + schemaText + '\'' +
                ", timestamp=" + timestamp +
                ", compatibility=" + compatibility +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaDto schemaDto = (SchemaDto) o;

        if (id != null ? !id.equals(schemaDto.id) : schemaDto.id != null) return false;
        if (name != null ? !name.equals(schemaDto.name) : schemaDto.name != null) return false;
        if (type != null ? !type.equals(schemaDto.type) : schemaDto.type != null) return false;
        if (description != null ? !description.equals(schemaDto.description) : schemaDto.description != null)
            return false;
        if (version != null ? !version.equals(schemaDto.version) : schemaDto.version != null) return false;
        if (schemaText != null ? !schemaText.equals(schemaDto.schemaText) : schemaDto.schemaText != null) return false;
        if (timestamp != null ? !timestamp.equals(schemaDto.timestamp) : schemaDto.timestamp != null) return false;
        return compatibility == schemaDto.compatibility;

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (description != null ? description.hashCode() : 0);
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (schemaText != null ? schemaText.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        result = 31 * result + (compatibility != null ? compatibility.hashCode() : 0);
        return result;
    }
}
