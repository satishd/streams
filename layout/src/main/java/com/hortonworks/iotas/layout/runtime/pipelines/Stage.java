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
package com.hortonworks.iotas.layout.runtime.pipelines;

import java.util.List;

/**
 *
 */
public class Stage {
    public final String name;
    public final List<Action> actions;
    public final Stage nextStage;

    // can have error handler which is kind of sink of errors generated from this stage

    public Stage(String name, List<Action> actions, Stage nextStage) {
        this.name = name;
        this.actions = actions;
        this.nextStage = nextStage;
    }


}
