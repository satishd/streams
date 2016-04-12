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
import com.hortonworks.iotas.layout.design.transform.EnrichmentTransform;
import com.hortonworks.iotas.layout.runtime.transform.TransformRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Enrichment adds an extra enriched message of original message's fields.
 */
public class EnrichmentTransformRuntime implements TransformRuntime {
    private static final Logger log = LoggerFactory.getLogger(EnrichmentTransformRuntime.class);

    private final EnrichmentTransform enrichmentTransform;

    private CachedDataProvider<Object, Object> cachedDataProvider;

    public EnrichmentTransformRuntime(EnrichmentTransform enrichmentTransform) {
        this.enrichmentTransform = enrichmentTransform;
    }

    public void prepare() {
        cachedDataProvider = new CachedDataProvider<Object, Object>(enrichmentTransform.getDataProvider(), enrichmentTransform.getMaxCacheSize(),
                enrichmentTransform.getEntryExpirationInterval(), enrichmentTransform.getEntryRefreshInterval());
        cachedDataProvider.prepare();
    }

    @Override
    public List<IotasEvent> execute(IotasEvent iotasEvent) {
        List<String> fieldsToBeEnriched = enrichmentTransform.getFieldsToBeEnriched();
        Map<String, Object> fieldsAndValues = iotasEvent.getFieldsAndValues();
        Map<String, Object> auxiliaryFieldsAndValues = iotasEvent.getAuxiliaryFieldsAndValues();
        Map<String, Object> enrichments = (Map<String, Object>) auxiliaryFieldsAndValues.get(EnrichmentTransform.ENRICHMENTS_FIELD_NAME);
        if (enrichments == null) {
            enrichments = new HashMap<>();
            auxiliaryFieldsAndValues.put(EnrichmentTransform.ENRICHMENTS_FIELD_NAME, enrichments);
        }
        for (String fieldName : fieldsToBeEnriched) {
            Object value = fieldsAndValues.get(fieldName);
            if (value != null) {
                Object enrichedValue = cachedDataProvider.get(value);
                log.debug("Enriched value [{}] for key [{}] with value [{}]", enrichedValue, fieldName, value);
                enrichments.put(fieldName, enrichedValue);
            } else {
                log.warn("Value in input event for key [{}] is null", fieldName);
            }
        }
        return Collections.singletonList(iotasEvent);
    }

}
