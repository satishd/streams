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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.hortonworks.iotas.catalog;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.storage.PrimaryKey;

import java.util.HashMap;
import java.util.Map;

/**
 * Jar configuration
 */
public class Jar extends AbstractStorable {
    public static final String NAME_SPACE = "jars";
    public static final String ID = "id";

    /**
     * Unique Id for a jar instance. This is the primary key column.
     */
    private Long id;

    /**
     * Human readable name.
     * (name, version) pair is unique constraint.
     */
    private String name;

    /**
     * Fully qualified class name that implements a given interface.
     */
    private String className;

    /**
     * Name of the jar in the configured storage.
     */
    private String storedFileName;

    /**
     * Jar version.
     * (name, version) pair is unique constraint.
     */
    private Long version;

    /**
     * Time at which this jar was created/updated.
     */
    private Long timestamp;

    /**
     * Extra information about the Jar.
     */
    private String auxiliaryInfo;

    @Override
    @JsonIgnore
    public String getNameSpace() {
        return NAME_SPACE;
    }

    @Override
    @JsonIgnore
    public PrimaryKey getPrimaryKey() {
        Map<Schema.Field, Object> fieldObjectMap = new HashMap<>();
        fieldObjectMap.put(new Schema.Field(ID, Schema.Type.LONG), this.id);
        return new PrimaryKey(fieldObjectMap);
    }

    /**
     * @return Unique Id for a jar instance, which is the primary key column.
     *
     */
    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    /**
     * @return Human readable name.
     */
    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    /**
     * @return Fully qualified class name that implements a given interface. This is an optional property.
     *
     */
    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    /**
     * @return Storage location of the jar.
     */
    public String getStoredFileName() {
        return storedFileName;
    }

    public void setStoredFileName(String storedFileName) {
        this.storedFileName = storedFileName;
    }

    /**
     * @return version of the Jar.
     */
    public Long getVersion() {
        return version;
    }

    public void setVersion(Long version) {
        this.version = version;
    }

    /**
     * @return the time at which this jar was created/updated.
     */
    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * @return Extra information about this Jar which is represented in String format.
     */
    public String getAuxiliaryInfo() {
        return auxiliaryInfo;
    }

    public void setAuxiliaryInfo(String auxiliaryInfo) {
        this.auxiliaryInfo = auxiliaryInfo;
    }

    @Override
    public String toString() {
        return "Jar{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", className='" + className + '\'' +
                ", storageName='" + storedFileName + '\'' +
                ", version=" + version +
                ", timestamp=" + timestamp +
                ", auxiliaryInfo='" + auxiliaryInfo + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Jar)) return false;

        Jar jar = (Jar) o;

        if (!id.equals(jar.id)) return false;
        if (!name.equals(jar.name)) return false;
        if (!className.equals(jar.className)) return false;
        if (!storedFileName.equals(jar.storedFileName)) return false;
        if (!version.equals(jar.version)) return false;
        if (!timestamp.equals(jar.timestamp)) return false;
        return auxiliaryInfo.equals(jar.auxiliaryInfo);

    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + className.hashCode();
        result = 31 * result + storedFileName.hashCode();
        result = 31 * result + version.hashCode();
        result = 31 * result + timestamp.hashCode();
        result = 31 * result + auxiliaryInfo.hashCode();
        return result;
    }

}
