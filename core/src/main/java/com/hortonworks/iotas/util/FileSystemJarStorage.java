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

import com.google.common.io.ByteStreams;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

/**
 * Implementation of JarStorage interface backed by local file system
 */
public class FileSystemJarStorage implements JarStorage {
    // the configuration keys
    public static final String CONFIG_DIRECTORY = "directory";

    // default to /tmp
    private String directory = "/tmp";

    @Override
    public void init(Map<String, String> config) {
        String dir;
        if ((dir = config.get(CONFIG_DIRECTORY)) != null) {
            directory = dir;
        }
    }

    /**
     *
     * @param inputStream stream to read the jar content from
     * @param name identifier of the jar file to be used later to retrieve
     *             using downloadJar
     *
     * @throws java.io.IOException
     */
    public String uploadJar (InputStream inputStream, String name) throws IOException {
        Path path = FileSystems.getDefault().getPath(directory, name);
        File file = path.toFile();
        if (!file.createNewFile()) {
            throw new IOException("File: ["+name+"] already exists");
        }
        try (OutputStream outputStream = new FileOutputStream(file)) {
            ByteStreams.copy(inputStream, outputStream);
        }
        return path.toString();
    }

    /**
     *
     * @param name identifier of the jar file to be downloaded that was first
     *             passed during uploadJar
     * @return InputStream representing the jar file
     * @throws java.io.IOException
     */
    public InputStream downloadJar (String name) throws IOException {
        java.nio.file.Path path = FileSystems.getDefault().getPath(directory, name);
        File file = path.toFile();
        return new FileInputStream(file);
    }

    @Override
    public boolean deleteJar(String name) throws IOException {
        java.nio.file.Path path = FileSystems.getDefault().getPath(directory, name);
        return Files.deleteIfExists(path);
    }

}
