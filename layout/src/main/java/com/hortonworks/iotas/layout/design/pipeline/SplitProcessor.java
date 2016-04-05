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
import com.hortonworks.iotas.layout.runtime.pipeline.SplitAction;

import java.util.Collections;

/**
 * - output streams list
 *      - containing schema, stream id.
 * - Splitter jar
 * - Splitter class to be loaded from jar using proxy class loader
 * - Add centralized jar storage utility so that any component can use those jars
 * todo - Use RulesProcessor as the only action with Split
 */
public class SplitProcessor extends RulesProcessor {

    private String jarId;

    public SplitProcessor(SplitAction splitAction) {
        final TrueRule trueRule = new TrueRule();
        trueRule.setActions(Collections.<Action>singletonList(splitAction));
        setRules(Collections.<Rule>singletonList(trueRule));
    }

    static class TrueRule extends Rule {
        public TrueRule() {
            setName("true-rule");
            setId(System.currentTimeMillis());
        }
    }

    public void setJar(String jarId) {
        this.jarId = jarId;
    }

}
