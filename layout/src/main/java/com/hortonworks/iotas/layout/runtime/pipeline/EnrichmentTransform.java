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

import com.hortonworks.iotas.common.Schema;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 *
 */
public class EnrichmentTransform<K, V> {

    public static final long DEFAULT_MAX_CACHE_SIZE = 1000;
    public static final long DEFAULT_ENTRY_EXPIRATION_INTERVAL = 60 * 5 * 1000;
    public static final long DEFAULT_ENTRY_REFRESH_INTERVAL = 60 * 5 * 1000;

    /**
     * Name of the enrichment
     */
    private final String name;

    /**
     * original fields to be enriched.
     */
    private final List<Schema.Field> fieldsToBeEnriched;

    /**
     * enriched field's with name and type.
     */
    private final Schema.Field outputField;

    /**
     * Used for lookups of enrich field values.
     */
    private final DataProvider<K, V> dataProvider;

    /**
     * maximum size of the cache
     */
    private long maxCacheSize = DEFAULT_MAX_CACHE_SIZE;

    /**
     * interval (in seconds) of an entry to be evicted from cache after it is loaded.
     */
    private long entryExpirationInterval = DEFAULT_ENTRY_EXPIRATION_INTERVAL;

    /**
     * interval(in seconds) of an entry after which the entry should be loaded from {@link DataProvider}.
     */
    private long entryRefreshInterval = DEFAULT_ENTRY_REFRESH_INTERVAL;

    public EnrichmentTransform(String name, List<Schema.Field> fieldsToBeEnriched, Schema.Field outputField, DataProvider<K, V> dataProvider) {
        this.name = name;
        this.fieldsToBeEnriched = fieldsToBeEnriched;
        this.outputField = outputField;
        this.dataProvider = dataProvider;
    }

    public void withMaxCacheSize(long maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
    }

    public void withEntryExpirationInterval(long entryExpirationInterval, TimeUnit timeUnit) {
        this.entryExpirationInterval = timeUnit.convert(entryExpirationInterval, TimeUnit.SECONDS);
    }

    public void withEntryRefreshInterval(long refreshInterval, TimeUnit timeUnit) {
        this.entryRefreshInterval = timeUnit.convert(refreshInterval, TimeUnit.SECONDS);
    }

    public String getName() {
        return name;
    }

    public List<Schema.Field> getFieldsToBeEnriched() {
        return fieldsToBeEnriched;
    }

    public Schema.Field getOutputField() {
        return outputField;
    }

    public DataProvider<K, V> getDataProvider() {
        return dataProvider;
    }

    public long getMaxCacheSize() {
        return maxCacheSize;
    }

    public long getEntryExpirationInterval() {
        return entryExpirationInterval;
    }

    public long getEntryRefreshInterval() {
        return entryRefreshInterval;
    }
}
