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

import com.hortonworks.iotas.layout.design.pipeline.Transform;
import com.hortonworks.iotas.layout.runtime.transform.TransformRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory to create {@link com.hortonworks.iotas.layout.runtime.transform.TransformRuntime} instances of a given {@link com.hortonworks.iotas.layout.design.pipeline.Transform}
 * todo change this class name
 */
public class TransformRuntimeFactory {
    private static Logger log = LoggerFactory.getLogger(TransformRuntimeFactory.class);

    private static TransformRuntimeFactory instance = new TransformRuntimeFactory();
    private static Map<Class<? extends Transform>, TransformRuntime.Factory> factories = new ConcurrentHashMap<>();

    private TransformRuntimeFactory() {
    }

    public static TransformRuntimeFactory get() {
        return instance;
    }

    public void register(Class<? extends Transform> klass, TransformRuntime.Factory factory) {
        factories.put(klass, factory);
    }

    public TransformRuntime create(Transform transform) {
        final TransformRuntime.Factory transformFactory = factories.get(transform.getClass());
        if(transformFactory == null) {
            log.error("No factory registered for [{}]", transform.getClass());
            throw new IllegalArgumentException("");
        }

        return transformFactory.create(transform);
    }
}
