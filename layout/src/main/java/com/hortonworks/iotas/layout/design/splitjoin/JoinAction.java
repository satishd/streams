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
package com.hortonworks.iotas.layout.design.splitjoin;

import com.hortonworks.iotas.layout.design.rule.action.Action;

/**
 * {@link Action} configuration for joining the events split by a {@link SplitProcessor}.
 * When {@code joinerClassName} is not given then {@link com.hortonworks.iotas.layout.runtime.splitjoin.DefaultJoiner} is used.
 */
public class JoinAction extends Action {

    private String jarId;
    private String joinerClassName;

    public JoinAction() {
    }

    public JoinAction(String jarId, String joinerClassName) {
        this.jarId = jarId;
        this.joinerClassName = joinerClassName;
    }

    public String getJarId() {
        return jarId;
    }

    public String getJoinerClassName() {
        return joinerClassName;
    }

    @Override
    public String toString() {
        return "JoinAction{" +
                "jarId='" + jarId + '\'' +
                ", joinerClassName='" + joinerClassName + '\'' +
                '}'+super.toString();
    }
}
