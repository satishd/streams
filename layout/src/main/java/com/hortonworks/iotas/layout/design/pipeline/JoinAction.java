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

import com.hortonworks.iotas.layout.design.rule.action.Action;

/**
 * {@link} Action configuration for for joining the events.
 *
 */
public class JoinAction extends Action {

    //todo do we really any other configuration?
    // yes, joiner class needed.
    private String jarId;
    private String joinerClassName;

    private JoinAction() {
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
