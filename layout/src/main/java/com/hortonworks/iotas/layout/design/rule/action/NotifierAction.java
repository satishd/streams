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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hortonworks.iotas.layout.design.rule.action;


/**
 * {@link Action} to send notifications.
 *
 */
public class NotifierAction extends Action {
    private boolean includeMeta = false;
    private String notifierName = "dummy";

    public NotifierAction() { }

    public boolean isIncludeMeta() {
        return includeMeta;
    }

    /**
     * Whether to include meta data (rule-id, event-id, datasource-id) in the output
     * IotasEvent header.
     */
    public void setIncludeMeta(boolean includeMeta) {
        this.includeMeta = includeMeta;
    }

    public String getNotifierName() {
        return notifierName;
    }

    public void setNotifierName(String notifierName) {
        this.notifierName = notifierName;
    }

    @Override
    public String toString() {
        return "NotifierAction{" +
                "includeMeta=" + includeMeta +
                ", notifierName='" + notifierName + '\'' +
                '}'+super.toString();
    }
}
