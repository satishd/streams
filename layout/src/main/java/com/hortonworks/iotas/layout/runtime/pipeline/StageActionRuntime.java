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
import com.hortonworks.iotas.common.Result;
import com.hortonworks.iotas.layout.design.pipeline.StageAction;
import com.hortonworks.iotas.layout.design.pipeline.Transform;
import com.hortonworks.iotas.layout.runtime.ActionRuntime;
import com.hortonworks.iotas.layout.runtime.TransformActionRuntime;
import com.hortonworks.iotas.layout.runtime.transform.TransformRuntime;

import java.util.ArrayList;
import java.util.List;

/**
 * {@link ActionRuntime} of a stage processor.
 *
 */
public class StageActionRuntime implements ActionRuntime {

    private final StageAction stageAction;
    private TransformActionRuntime transformActionRuntime;

    // register factories
    // todo this can be moved to startup listener
    static {
        TransformRuntimeFactory.get().register(EnrichmentTransform.class, new TransformRuntime.Factory() {
            @Override
            public TransformRuntime create(Transform transform) {
                return new EnrichmentTransformRuntime((EnrichmentTransform) transform);
            }
        });
    }

    public StageActionRuntime(StageAction stageAction) {
        this.stageAction = stageAction;
    }

    public void prepare() {
        final List<Transform> transforms = stageAction.getTransforms();
        if(stageAction.getOutputStreams().size() != 1) {
            throw new RuntimeException("Stage can only have one output stream.");
        }
        String outputStream = stageAction.getOutputStreams().get(0);
        transformActionRuntime = new TransformActionRuntime(outputStream, getTransformRuntimes(transforms));
    }

    private List<TransformRuntime> getTransformRuntimes(List<Transform> transforms) {
        List<TransformRuntime> transformRuntimes = new ArrayList<>();
        for (Transform transform : transforms) {
            transformRuntimes.add(TransformRuntimeFactory.get().create(transform));
        }

        return transformRuntimes;
    }

    @Override
    public List<Result> execute(IotasEvent input) {
        return transformActionRuntime.execute(input);
    }

    @Override
    public List<String> getOutputStreams() {
        return transformActionRuntime.getOutputStreams();
    }
}
