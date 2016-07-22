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
package com.hortonworks.iotas.schemaregistry.serde;

import com.hortonworks.iotas.schemaregistry.SchemaInfo;
import com.hortonworks.iotas.schemaregistry.client.SchemaRegistryClient;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

/**
 *
 */
public abstract class AbstractSnapshotSerializer<O> implements SnapshotSerializer<InputStream, O, SchemaInfo> {
    private final SchemaRegistryClient schemaRegistryClient;

    protected AbstractSnapshotSerializer(SchemaRegistryClient schemaRegistryClient) {
        this.schemaRegistryClient = schemaRegistryClient;
    }

    protected abstract void doSerialize(InputStream input, OutputStream outputStream, SchemaInfo schema) throws SerDeException;

    @Override
    public final O serialize(InputStream input, SchemaInfo schema) throws SerDeException {
        throw new UnsupportedOperationException("This method is not supported");
    }

    @Override
    public final void serialize(InputStream input, OutputStream outputStream, SchemaInfo schema) throws SerDeException {

        // write schema id
        try {
            outputStream.write(ByteBuffer.allocate(8).putLong(getSchemaId(schema)).array());
        } catch (IOException e) {
            throw new SerDeException(e);
        }

        doSerialize(input, outputStream, schema);
    }

    /**
     * Returns ID stored for the given schemaInfo instance
     *
     * @param schemaInfo
     * @return
     */
    protected long getSchemaId(SchemaInfo schemaInfo) {
        return schemaRegistryClient.add(schemaInfo).getId();
    };
}
