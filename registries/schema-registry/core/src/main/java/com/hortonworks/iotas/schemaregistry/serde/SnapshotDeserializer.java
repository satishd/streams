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

import java.io.InputStream;
import java.util.Map;

/**
 * This parser returns {@link Map} of field and values after parsing the given payload.
 * <p>
 *
 * @param <S> Schema representation class
 * @param <O> Output type of the deserialized content.
 */
public interface SnapshotDeserializer<S, O> {

    /**
     * Returns output {@code O} of field and values generated after deserializing the given {@code payloadInputStream}
     *
     * @param payloadInputStream
     * @param schema
     * @return
     */
    public O deserialize(InputStream payloadInputStream, S schema);

}
