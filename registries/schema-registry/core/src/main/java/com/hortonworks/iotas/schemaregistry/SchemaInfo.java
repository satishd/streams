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
import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.catalog.AbstractStorable;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class SchemaInfo extends AbstractStorable {
    public static final String NAME_SPACE = "schema_info";
    public static final String ID = "id";
    public static final String NAME = "name";
    public static final String SCHEMA_TEXT = "schemaText";
    public static final String VERSION = "version";
    public static final String TIMESTAMP = "timestamp";
    public static final String COMPATIBILITY = "compatibility";
    public static final String TYPE="type";
    /**
     * Unique ID generated for this component.
     */
    private Long id;

    /**
     * Given name of the schema. (name, version) pair is unique constraint.
     */
    private String name;

    /**
     * What schema will {@code Parser} be returned by parser's parse method.
     */
    private String schemaText;

    /**
     * Current version of the schema. (name, version) pair is unique constraint.
     */
    private Integer version;

    /**
     * Time at which this schema was created/updated.
     */
    private Long timestamp;

    /**
     * Schema type which can be avro, protobuf or json etc
     */
    private String type;

    /**
     * Compatibility of this schema instance
     */
    private SchemaProvider.Compatibility compatibility = SchemaProvider.Compatibility.NONE;

    public SchemaInfo() {
    }

    public SchemaInfo(String name, String type) {
        this.name = name;
        this.type = type;
    }

    public SchemaInfo(SchemaInfo givenSchemaInfo) {
        id = givenSchemaInfo.id;
        name = givenSchemaInfo.name;
        type = givenSchemaInfo.type;
        version = givenSchemaInfo.version;
        schemaText = givenSchemaInfo.schemaText;
        timestamp = givenSchemaInfo.timestamp;
        compatibility = givenSchemaInfo.compatibility;
    }

    @Override
    @JsonIgnore
    public String getNameSpace() {
        return NAME_SPACE;
    }

    @Override
    @JsonIgnore
    public PrimaryKey getPrimaryKey() {
        Map<Schema.Field, Object> values = new HashMap<>();
        values.put(new Schema.Field(ID, Schema.Type.LONG), id);
        return new PrimaryKey(values);
    }

    @Override
    @JsonIgnore
    public Schema getSchema() {
        return Schema.of(
                Schema.Field.of(ID, Schema.Type.LONG),
                Schema.Field.of(NAME, Schema.Type.STRING),
                Schema.Field.of(SCHEMA_TEXT, Schema.Type.STRING),
                Schema.Field.of(TYPE, Schema.Type.STRING),
                Schema.Field.of(VERSION, Schema.Type.LONG),
                Schema.Field.of(TIMESTAMP, Schema.Type.LONG)
        );
    }

    @Override
    public Map<String, Object> toMap() {
        Map<String, Object> values = super.toMap();
        values.put(COMPATIBILITY, compatibility.name());
        return values;
    }

    @Override
    public Storable fromMap(Map<String, Object> map) {
        String compatibilityName = (String) map.remove(COMPATIBILITY);
        compatibility = SchemaProvider.Compatibility.valueOf(compatibilityName);
        super.fromMap(map);
        return this;
    }

    @Override
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getSchemaText() {
        return schemaText;
    }

    public void setSchemaText(String schemaText) {
        this.schemaText = schemaText;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public SchemaProvider.Compatibility getCompatibility() {
        return compatibility;
    }

    public void setCompatibility(SchemaProvider.Compatibility compatibility) {
        this.compatibility = compatibility;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SchemaInfo that = (SchemaInfo) o;

        if (id != null ? !id.equals(that.id) : that.id != null) return false;
        if (name != null ? !name.equals(that.name) : that.name != null) return false;
        if (schemaText != null ? !schemaText.equals(that.schemaText) : that.schemaText != null) return false;
        if (version != null ? !version.equals(that.version) : that.version != null) return false;
        if (timestamp != null ? !timestamp.equals(that.timestamp) : that.timestamp != null) return false;
        if (type != null ? !type.equals(that.type) : that.type != null) return false;
        return compatibility == that.compatibility;

    }

    @Override
    public int hashCode() {
        int result = id != null ? id.hashCode() : 0;
        result = 31 * result + (name != null ? name.hashCode() : 0);
        result = 31 * result + (schemaText != null ? schemaText.hashCode() : 0);
        result = 31 * result + (version != null ? version.hashCode() : 0);
        result = 31 * result + (timestamp != null ? timestamp.hashCode() : 0);
        result = 31 * result + (type != null ? type.hashCode() : 0);
        result = 31 * result + (compatibility != null ? compatibility.hashCode() : 0);
        return result;
    }
}
