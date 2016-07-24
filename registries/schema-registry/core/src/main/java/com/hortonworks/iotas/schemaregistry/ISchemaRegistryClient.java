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

import java.util.Collection;

/**
 *
 */
public interface ISchemaRegistryClient {

    public SchemaInfo add(SchemaInfo schemaInfo);

    public Collection<SchemaInfo> list();

    public Collection<SchemaInfo> list(String type);

    public SchemaInfo get(String type, String name, Integer version);

    public SchemaInfo get(Long id);

    public SchemaInfo getLatest(String type, String name);

    public Collection<SchemaInfo> get(String type, String name);

    public boolean isCompatibleWithLatest(String type, String toSchemaText, String existingSchemaName);

    public boolean isCompatible(String type, String name, Integer existingSchemaVersion, Integer toSchemaVersion);
}
