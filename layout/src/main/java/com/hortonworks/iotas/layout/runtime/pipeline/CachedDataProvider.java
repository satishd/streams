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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * This class creates a loadable cache for given backing {@link DataProvider} with caching configuration like maximum size, expiration interval
 * and refresh interval.
 */
public class CachedDataProvider<K, V> implements DataProvider<K, V> {

    private final DataProvider<K, V> backedDataProvider;
    private long maxCacheSize;
    private long entryExpirationInterval;
    private long refreshInterval;
    private LoadingCache<K, V> loadingCache;

    public CachedDataProvider(DataProvider<K, V> backedDataProvider, long maxCacheSize, long entryExpirationInterval, long entryRefreshInterval) {
        this.backedDataProvider = backedDataProvider;
        this.maxCacheSize = maxCacheSize;
        this.entryExpirationInterval = entryExpirationInterval;
        this.refreshInterval = entryRefreshInterval;
    }

    @Override
    public void prepare() {
        backedDataProvider.prepare();
        loadingCache =
                CacheBuilder.newBuilder()
                        .maximumSize(maxCacheSize)
                        .refreshAfterWrite(refreshInterval, TimeUnit.SECONDS)
                        .expireAfterWrite(entryExpirationInterval, TimeUnit.SECONDS)
                        .build(new CacheLoader<K, V>() {
                            @Override
                            public V load(K key) throws Exception {
                                return backedDataProvider.get(key);
                            }
                        });

    }

    @Override
    public V get(K key) {
        try {
            return loadingCache.get(key);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cleanup() {
        loadingCache.cleanUp();
        backedDataProvider.cleanup();
    }

}
