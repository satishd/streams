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
package com.hortonworks.iotas.layout.runtime.rule.action;

import com.hortonworks.iotas.common.IotasEvent;
import com.hortonworks.iotas.common.Result;
import com.hortonworks.iotas.layout.design.rule.action.Action;
import com.hortonworks.iotas.layout.design.rule.action.NotifierAction;
import com.hortonworks.iotas.layout.design.transform.ProjectionTransform;
import com.hortonworks.iotas.layout.runtime.RuntimeService;
import com.hortonworks.iotas.layout.runtime.TransformActionRuntime;
import com.hortonworks.iotas.layout.runtime.transform.AddHeaderTransformRuntime;
import com.hortonworks.iotas.layout.runtime.transform.MergeTransformRuntime;
import com.hortonworks.iotas.layout.runtime.transform.ProjectionTransformRuntime;
import com.hortonworks.iotas.layout.runtime.transform.SubstituteTransformRuntime;
import com.hortonworks.iotas.layout.runtime.transform.TransformRuntime;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public class NotifierActionRuntime implements ActionRuntime {

    private final NotifierAction notifierAction;
    private TransformActionRuntime transformActionRuntime;
    private String outputStream;

    public NotifierActionRuntime(NotifierAction notifierAction) {
        this.notifierAction = notifierAction;
    }

    @Override
    public void prepare(ActionRuntimeContext actionRuntimeContext) {
        outputStream = actionRuntimeContext.getRule().getOutputStreamNameForAction(notifierAction);
        transformActionRuntime = new TransformActionRuntime(outputStream, getNotificationTransforms(notifierAction, actionRuntimeContext.getRule().getId()));
    }

    @Override
    public List<Result> execute(IotasEvent input) {
        return transformActionRuntime.execute(input);
    }

    @Override
    public Set<String> getOutputStreams() {
        return Collections.singleton(outputStream);
    }


    /**
     * Returns the necessary transforms to perform based on the action.
     */
    private List<TransformRuntime> getNotificationTransforms(NotifierAction action, Long ruleId) {
        List<TransformRuntime> transformRuntimes = new ArrayList<>();
        if (action.getOutputFieldsAndDefaults() != null && !action.getOutputFieldsAndDefaults().isEmpty()) {
            transformRuntimes.add(new MergeTransformRuntime(action.getOutputFieldsAndDefaults()));
            transformRuntimes.add(new SubstituteTransformRuntime(action.getOutputFieldsAndDefaults().keySet()));
            transformRuntimes.add(new ProjectionTransformRuntime(new ProjectionTransform("projection-" + ruleId, action.getOutputFieldsAndDefaults().keySet())));
        }

        Map<String, Object> headers = new HashMap<>();
        headers.put(AddHeaderTransformRuntime.HEADER_FIELD_NOTIFIER_NAME, action.getNotifierName());
        headers.put(AddHeaderTransformRuntime.HEADER_FIELD_RULE_ID, ruleId);
        transformRuntimes.add(new AddHeaderTransformRuntime(headers));

        return transformRuntimes;
    }

    public static class Factory implements RuntimeService.Factory<ActionRuntime, Action> {
        @Override
        public ActionRuntime create(Action action) {
            return new NotifierActionRuntime((NotifierAction) action);
        }
    }

}
