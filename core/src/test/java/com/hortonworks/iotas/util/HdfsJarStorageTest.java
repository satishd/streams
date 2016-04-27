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

import org.apache.commons.io.IOUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 */
public class HdfsJarStorageTest {
    private static final String HDFS_DIR = "/tmp/test-hdfs";
    private HdfsJarStorage jarStorage;

    @Before
    public void setUp() throws Exception {
        jarStorage = new HdfsJarStorage();
    }

    @Test(expected = RuntimeException.class)
    public void testInitWithoutFsUrl() throws Exception {
        jarStorage.init(new HashMap<String, String>());
    }

    @Test
    public void testUploadJar() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put(HdfsJarStorage.CONFIG_FSURL, "file:///");
        jarStorage.init(config);

        File file = File.createTempFile("test", ".tmp");
        file.deleteOnExit();

        List<String> lines = Arrays.asList("test-line-1", "test-line-2");
        Files.write(file.toPath(), lines, Charset.forName("UTF-8"));
        String jarFileName = "test.jar";

        jarStorage.deleteJar(jarFileName);

        jarStorage.uploadJar(new FileInputStream(file), jarFileName);

        InputStream inputStream = jarStorage.downloadJar(jarFileName);
        List<String> actual = IOUtils.readLines(inputStream);
        Assert.assertEquals(lines, actual);
    }

    @Test
    public void testUploadJarWithDir() throws Exception {
        Map<String, String> config = new HashMap<>();
        config.put(HdfsJarStorage.CONFIG_FSURL, "file:///");
        config.put(HdfsJarStorage.CONFIG_DIRECTORY, HDFS_DIR);
        jarStorage.init(config);

        File file = File.createTempFile("test", ".tmp");
        file.deleteOnExit();

        List<String> lines = Arrays.asList("test-line-1", "test-line-2");
        Files.write(file.toPath(), lines, Charset.forName("UTF-8"));
        String jarFileName = "test.jar";

        jarStorage.deleteJar(jarFileName);

        jarStorage.uploadJar(new FileInputStream(file), jarFileName);

        InputStream inputStream = jarStorage.downloadJar(jarFileName);
        List<String> actual = IOUtils.readLines(inputStream);
        Assert.assertEquals(lines, actual);
    }

}
