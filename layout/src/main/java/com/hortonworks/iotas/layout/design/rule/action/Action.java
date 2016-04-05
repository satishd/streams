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
package com.hortonworks.iotas.layout.design.rule.action;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Action that is at the end of the chain of execution. Once this action is complete, this rule will not be evaluated anymore.
 * The actions performed by this rule will not interact directly with any other components of the rule system, e.g., other rules,
 * components, sinks, ...
 *
 */
public class Action implements Serializable {
    protected String name = "default";
    protected Map<String, Object> outputFieldsAndDefaults = new HashMap<>();

    /**
     * The name of the output fields and the default values for them
     * to be emitted as a part of this action.
     */
    public void setOutputFieldsAndDefaults(Map<String, Object> outputFieldsAndDefaults) {
        this.outputFieldsAndDefaults = outputFieldsAndDefaults;
    }

    public Map<String, Object> getOutputFieldsAndDefaults() {
        return outputFieldsAndDefaults;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "Action{" +
                "name='" + name + '\'' +
                ", outputFieldsAndDefaults=" + outputFieldsAndDefaults +
                '}';
    }

}
