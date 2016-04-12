/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.hortonworks.iotas.layout.runtime.pipeline;

/**
 * Data provider for {@link com.hortonworks.iotas.layout.design.pipeline.Transform} which can be used for lookups.
 */
public interface DataProvider<K, V> {

    /**
     * Prepare resources which can be used in retrieving values from data store.
     */
    public void prepare(); //Config config);

    /**
     * Retrieves a value for a given key from a data store.
     *
     * @param key
     */
    public V get(K key);

    /**
     * cleanup any resources held by this instance.
     */
    public void cleanup();

}
