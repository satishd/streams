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
package com.hortonworks.iotas.layout.design.pipeline;

import com.hortonworks.iotas.layout.design.component.RulesProcessor;
import com.hortonworks.iotas.layout.design.rule.Rule;
import com.hortonworks.iotas.layout.design.rule.action.Action;
import com.hortonworks.iotas.layout.runtime.transform.TransformRuntime;

import java.util.Collections;
import java.util.List;

/**
 * Stage has a list of transforms to be applied and send the out put to given stream.
 */
public class StageProcessor extends RulesProcessor {

    public StageProcessor(List<Transform> transforms) {
        final Rule rule = new Rule() {{
            setName("split-true-rule");
            setId(System.currentTimeMillis());
        }};

        rule.setActions(Collections.<Action>singletonList(new StageAction(transforms)));
        setRules(Collections.singletonList(rule));
    }

}
