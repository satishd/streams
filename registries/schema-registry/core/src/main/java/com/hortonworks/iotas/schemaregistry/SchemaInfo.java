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

import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.storage.PrimaryKey;
import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.catalog.AbstractStorable;

import java.util.HashMap;
import java.util.Map;

/**
 * todo:
 *  - Add registration of producers and consumers of this schema.
 *  - Whenever there is an update with this schema, consumers should know about this.
 */
public class SchemaInfo extends AbstractStorable {
    public static final String NAME_SPACE = "schema_info";
    public static final String ID = "id";
    public static final String NAME = "name";
    public static final String SCHEMA_TEXT = "schemaText";
    public static final String VERSION = "version";
    public static final String TIMESTAMP = "timestamp";
    public static final String COMPATIBILITY = "compatibility";

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

    private Compatibility compatibility = Compatibility.NONE;

    public enum Compatibility {
        NONE(),
        BACKWARD(),
        FORWARD()
    }

    public SchemaInfo() {
    }

    public SchemaInfo(String name, Integer version) {
        this.name = name;
        this.version = version;
    }

    @Override
    public String getNameSpace() {
        return NAME_SPACE;
    }

    @Override
    public PrimaryKey getPrimaryKey() {
        Map<Schema.Field, Object> values = new HashMap<>();
        values.put(new Schema.Field(ID, Schema.Type.LONG), id);
        return new PrimaryKey(values);
    }

    @Override
    public Schema getSchema() {
        return Schema.of(
                Schema.Field.of(ID, Schema.Type.LONG),
                Schema.Field.of(NAME, Schema.Type.STRING),
                Schema.Field.of(SCHEMA_TEXT, Schema.Type.STRING),
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
        compatibility = Compatibility.valueOf(compatibilityName);
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

    public Compatibility getCompatibility() {
        return compatibility;
    }

    public void setCompatibility(Compatibility compatibility) {
        this.compatibility = compatibility;
    }
}
