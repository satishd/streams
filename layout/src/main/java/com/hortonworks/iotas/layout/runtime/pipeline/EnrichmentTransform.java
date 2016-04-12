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
import com.hortonworks.iotas.layout.design.pipeline.Transform;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * This class can be used to configure enrichment transform which can be used in any {@link com.hortonworks.iotas.layout.design.rule.action.Action}
 * of a rule based processor.
 *
 */
public class EnrichmentTransform<K, V> extends Transform {

    public static final long DEFAULT_MAX_CACHE_SIZE = 1000;
    public static final long DEFAULT_ENTRY_EXPIRATION_INTERVAL = 60 * 5 * 1000;
    public static final long DEFAULT_ENTRY_REFRESH_INTERVAL = 60 * 5 * 1000;

    /**
     * original fields to be enriched.
     */
    private final List<String> fieldsToBeEnriched;

    /**
     * Used for lookups of enrich field values.
     */
    private final DataProvider<Object, Object> dataProvider;

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

    public EnrichmentTransform(String name, List<String> fieldsToBeEnriched, DataProvider<Object, Object> dataProvider) {
        super(name);
        this.fieldsToBeEnriched = fieldsToBeEnriched;
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

    public List<String> getFieldsToBeEnriched() {
        return Collections.unmodifiableList(fieldsToBeEnriched);
    }

    public DataProvider<Object, Object> getDataProvider() {
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
