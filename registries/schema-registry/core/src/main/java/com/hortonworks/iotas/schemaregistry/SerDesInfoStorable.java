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
 * This class is about entity representation to store serializer or deserializer information for a given schema.
 */
public abstract class SerDesInfoStorable extends AbstractStorable {
    public static final String NAME_SPACE = "schema_serdes_info";
    public static final String ID = "id";
    public static final String DESCRIPTION = "description";
    public static final String NAME = "name";
    public static final String CLASS_NAME = "className";
    public static final String FILE_ID = "fileId";
    public static final String SERIALIZER = "isSerializer";
    public static final String TIMESTAMP = "timestamp";

    public static final Schema SCHEMA = Schema.of(
            Schema.Field.of(ID, Schema.Type.LONG),
            Schema.Field.of(NAME, Schema.Type.STRING),
            Schema.Field.of(DESCRIPTION, Schema.Type.STRING),
            Schema.Field.of(TIMESTAMP, Schema.Type.LONG),
            Schema.Field.of(SERIALIZER, Schema.Type.BOOLEAN),
            Schema.Field.of(CLASS_NAME, Schema.Type.STRING),
            Schema.Field.of(FILE_ID, Schema.Type.STRING)
    );

    /**
     * Unique ID generated for this component.
     */
    protected Long id;

    /**
     * Description about this serializer instance
     */
    private String description;

    /**
     * Name of the serializer
     */
    private String name;

    /**
     * Id of the file stored.
     */
    private String fileId;

    /**
     * Class name of serializer or deserializer
     */
    private String className;

    protected  boolean isSerializer;

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
        return SCHEMA;
    }

    @Override
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
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

    public String getFileId() {
        return fileId;
    }

    public void setFileId(String fileId) {
        this.fileId = fileId;
    }

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public boolean isSerializer() {
        return isSerializer;
    }
}
