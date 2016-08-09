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
package com.hortonworks.iotas.schemaregistry;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.hortonworks.iotas.storage.StorageManager;
import com.hortonworks.iotas.storage.StorageProviderConfiguration;
import io.dropwizard.Application;
import io.dropwizard.setup.Environment;
import org.glassfish.jersey.media.multipart.MultiPartFeature;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;

/**
 *
 */
public class SchemaRegistryApplication extends Application<SchemaRegistryConfiguration> {

    @Override
    public void run(SchemaRegistryConfiguration configuration, Environment environment) throws Exception {
        environment.jersey().register(MultiPartFeature.class);
        StorageManager storageManager = getStorageManager(configuration.getStorageProviderConfiguration());
        Collection<? extends SchemaProvider> schemaProviders = getSchemaProviders(configuration.getSchemaProviderClasses());
        ISchemaRegistry schemaRegistry = new DefaultSchemaRegistry(storageManager, schemaProviders);
        //todo should be moved to resource initialization callback method
        schemaRegistry.init(Collections.<String, Object>emptyMap());
        environment.jersey().register(new SchemaRegistryCatalog(schemaRegistry));
    }

    private Collection<? extends SchemaProvider> getSchemaProviders(Collection<String> schemaProviderClassNames) {
        return Collections2.transform(schemaProviderClassNames, new Function<String, SchemaProvider>() {
            @Nullable
            @Override
            public SchemaProvider apply(@Nullable String className) {
                try {
                    return (SchemaProvider) Class.forName(className).newInstance();
                } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
                    throw new IllegalArgumentException("Given class ["+className+"] should be a sub class of "+SchemaProvider.class.getName());
                }
            }
        });
    }

    private StorageManager getStorageManager(StorageProviderConfiguration storageProviderConfiguration) {
        final String providerClass = storageProviderConfiguration.getProviderClass();
        StorageManager storageManager = null;
        try {
            storageManager = (StorageManager) Class.forName(providerClass).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        storageManager.init(storageProviderConfiguration.getProperties());

        return storageManager;
    }

    public static void main(String[] args) throws Exception {
        SchemaRegistryApplication schemaRegistryApplication = new SchemaRegistryApplication();
        schemaRegistryApplication.run(args);
    }

}
