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
package com.hortonworks.iotas.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * Interface abstracting the upload and download of jars (like parser, split/join or custom processor) for IoTaS.
 * IoTaS will provide a default file system based implementation which can be
 * swapped by another implementation using jarStorageImplementationClass
 * property in the iotas.yaml
 */
public interface JarStorage {

    String DEFAULT_DIR = "/tmp/iotas-jars";

    /**
     * The jar storage can be initialized with a set of key/value pairs.
     *
     * @param config the config specific to implementation
     */
    void init(Map<String, String> config);

    /**
     * @param inputStream stream to read the jar content from
     * @param name        identifier of the jar file to be used later to retrieve
     *                    using downloadJar
     * @return the path where the file was uploaded
     * @throws java.io.IOException
     */
    String uploadJar(InputStream inputStream, String name) throws IOException;

    /**
     *
     * @param name identifier of the jar file to be downloaded that was first
     *             passed during uploadJar
     * @return InputStream representing the jar file
     * @throws java.io.IOException
     */
    InputStream downloadJar(String name) throws IOException;

    /**
     * Deletes file with the given name.
     *
     * @param name file name
     * @return true if delete operation is successful.
     * @throws IOException
     */
    boolean deleteJar(String name) throws IOException;

}
