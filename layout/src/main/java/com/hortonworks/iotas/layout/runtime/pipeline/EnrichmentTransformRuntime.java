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

import com.hortonworks.iotas.common.IotasEvent;
import com.hortonworks.iotas.layout.runtime.transform.TransformRuntime;

import java.util.List;

/**
 * Enrichment adds an extra enriched message of original message's fields or already enriched fields.
 */
public class EnrichmentTransformRuntime<K, V> implements TransformRuntime {

    private final EnrichmentTransform<K, V> enrichmentTransform;
    private CachedDataProvider<K, V> cachedDataProvider;

    public EnrichmentTransformRuntime(EnrichmentTransform<K, V> enrichmentTransform) {
        this.enrichmentTransform = enrichmentTransform;
    }

    public void prepare() {
        cachedDataProvider = new CachedDataProvider<>(enrichmentTransform.getDataProvider(), enrichmentTransform.getMaxCacheSize(),
                enrichmentTransform.getEntryExpirationInterval(), enrichmentTransform.getEntryRefreshInterval());
        cachedDataProvider.prepare();
    }

    @Override
    public List<IotasEvent> execute(IotasEvent iotasEvent) {
        //todo
        return null;
    }

}
