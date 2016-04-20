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
package com.hortonworks.iotas.layout.runtime.splitjoin;

import com.hortonworks.iotas.common.IotasEvent;
import com.hortonworks.iotas.common.IotasEventImpl;
import com.hortonworks.iotas.common.Result;
import com.hortonworks.iotas.layout.design.splitjoin.JoinAction;
import com.hortonworks.iotas.layout.design.splitjoin.SplitAction;
import com.hortonworks.iotas.layout.design.splitjoin.StageAction;
import com.hortonworks.iotas.layout.design.transform.EnrichmentTransform;
import com.hortonworks.iotas.layout.design.transform.Transform;
import com.hortonworks.iotas.layout.runtime.transform.TransformDataProvider;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Tests related to split/join/stage processors.
 */
public class SplitJoinTest {

    @Test
    public void testSplitJoinProcessors() throws Exception {
        String[] outputStreams = {"stream-1", "stream-2", "stream-3"};

        final SplitAction splitAction = new SplitAction();
        splitAction.setOutputStreams(Arrays.asList(outputStreams));
        SplitActionRuntime splitActionRuntime = new SplitActionRuntime(splitAction);
        splitActionRuntime.prepare();

        IotasEvent iotasEvent = createRootEvent();
        final List<Result> results = splitActionRuntime.execute(iotasEvent);

        final JoinAction joinAction = new JoinAction();
        joinAction.setOutputStreams(Collections.singletonList("output-stream"));
        JoinActionRuntime joinActionRuntime = new JoinActionRuntime(joinAction);
        joinActionRuntime.prepare();

        List<Result> effectiveResult = null;
        for (Result result : results) {
            for (IotasEvent event : result.events) {
                List<Result> processedResult = joinActionRuntime.execute(event);
                if(processedResult != null ) {
                    effectiveResult = processedResult;
                }
            }
        }

        Assert.assertNotNull(effectiveResult);
    }

    @Test
    public void testStageProcessor() {
        final String enrichFieldName = "foo";
        final String enrichedValue = "foo-enriched-value";

        Map<Object, Object> map = new HashMap<Object, Object>(){{
            put("foo-value", enrichedValue);}};
        TransformDataProvider<Object, Object> transformDataProvider = createDataProvider(map);
        EnrichmentTransform enrichmentTransform = new EnrichmentTransform("enricher", Collections.singletonList(enrichFieldName), transformDataProvider);
        StageAction stageAction = new StageAction(Collections.<Transform>singletonList(enrichmentTransform));
        stageAction.setOutputStreams(Collections.singletonList("output-stream"));

        StageActionRuntime stageActionRuntime = new StageActionRuntime(stageAction);
        stageActionRuntime.prepare();

        final List<Result> results = stageActionRuntime.execute(createRootEvent());
        for (Result result : results) {
            for (IotasEvent event : result.events) {
                final Map<Object, Object> enrichments = (Map<Object, Object>) event.getAuxiliaryFieldsAndValues().get(EnrichmentTransform.ENRICHMENTS_FIELD_NAME);
                Assert.assertEquals(enrichments.get(enrichFieldName), enrichedValue);
            }
        }
    }

    public static TransformDataProvider<Object, Object> createDataProvider(final Map<Object, Object> map) {
        return new TransformDataProvider<Object, Object>() {
            @Override
            public void prepare() {
            }

            @Override
            public Object get(Object key) {
                return map.get(key);
            }

            @Override
            public void cleanup() {
                map.clear();
            }
        };
    }

    private IotasEvent createRootEvent() {
        Map<String, Object> fieldValues = new HashMap<String, Object>(){{put("foo", "foo-value"); put("bar", "bar-"+System.currentTimeMillis());}};

        return new IotasEventImpl(fieldValues, "ds-1", UUID.randomUUID().toString(), Collections.<String, Object>emptyMap(), "source-stream");
    }
}
