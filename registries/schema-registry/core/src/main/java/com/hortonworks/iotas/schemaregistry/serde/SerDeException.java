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
package com.hortonworks.iotas.schemaregistry.serde;

/**
 * This Exception is thrown when any errors occur during serialization/deserialization of a given payload.
 */
public class SerDeException extends RuntimeException {

    public SerDeException() {
    }

    public SerDeException(String message) {
        super(message);
    }

    public SerDeException(String message, Throwable cause) {
        super(message, cause);
    }

    public SerDeException(Throwable cause) {
        super(cause);
    }

    public SerDeException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
