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
package com.hortonworks.iotas.storage.atlas;

import com.hortonworks.iotas.storage.Storable;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class StorableAbstractFactory {
    private Map<String, IStorableFactory> storableFactories = new HashMap<>();

    public StorableAbstractFactory(Collection<IStorableFactory> storableFactories) {
        for (IStorableFactory storableFactory : storableFactories) {
            this.storableFactories.put(storableFactory.getNameSpace(), storableFactory);
        }
    }

    public Storable create(String nameSpace) {
        if(!storableFactories.containsKey(nameSpace)) {
            throw new IllegalArgumentException("No factory supported with the given namespace: "+nameSpace);
        }

        return storableFactories.get(nameSpace).create();
    }
}
