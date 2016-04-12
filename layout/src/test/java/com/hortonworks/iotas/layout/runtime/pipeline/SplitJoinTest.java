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
import com.hortonworks.iotas.common.IotasEventImpl;
import com.hortonworks.iotas.common.Result;
import com.hortonworks.iotas.common.Schema;
import com.hortonworks.iotas.layout.design.component.Stream;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Tests related to split/join processors.
 */
public class SplitJoinTest {

    private static Schema outputSchema = Schema.of(new Schema.Field("foo", Schema.Type.STRING),
            new Schema.Field("bar", Schema.Type.STRING));

    @Test
    public void testSplitJoinProcessors() throws Exception {
        String[] outputStreams = {"stream-1", "stream-2", "stream-3"};

        final SplitAction splitAction = new SplitAction();
        splitAction.setOutputStreams(Arrays.asList(outputStreams));
        SplitActionRuntime splitActionRuntime = new SplitActionRuntime(splitAction);
        splitActionRuntime.prepare();

        IotasEvent iotasEvent = createRootEvent();
        final List<Result> results = splitActionRuntime.execute(iotasEvent);

        JoinActionRuntime joinActionRuntime = new JoinActionRuntime("output", new JoinAction(null , null));
//        JoinProcessorRuntime joinProcessorRuntime = new JoinProcessorRuntime();
//
//        joinProcessorRuntime.setOutputStream(new Stream("output", outputSchema));
//
//        for (Result result : results) {
//            joinProcessorRuntime.addIncomingStream(result.stream);
//        }

        List<Result> effectiveResult = null;
        for (Result result : results) {
            for (IotasEvent event : result.events) {
                List<Result> processedResult = joinActionRuntime.execute(event);
                if(processedResult != null ) {
                    effectiveResult = processedResult;
                }
            }
        }

        System.out.println("####### effectiveResult = " + effectiveResult);
        //todo assert the effectiveResult.
    }

    private IotasEvent createRootEvent() {
        Map<String, Object> fieldValues = new HashMap<String, Object>(){{put("foo", "foo-"+System.currentTimeMillis()); put("bar", "bar-"+System.currentTimeMillis());}};

        return new IotasEventImpl(fieldValues, "ds-1", UUID.randomUUID().toString(), Collections.<String, Object>emptyMap(), "source-stream");
    }
}
