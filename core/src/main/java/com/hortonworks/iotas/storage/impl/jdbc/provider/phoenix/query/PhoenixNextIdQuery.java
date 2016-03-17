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
package com.hortonworks.iotas.storage.impl.jdbc.provider.phoenix.query;

import java.util.UUID;

/**
 *
 */
public class PhoenixNextIdQuery {
    private String namespace;

    public PhoenixNextIdQuery(String namespace) {
        this.namespace = namespace;
    }

    public Long getNextID() {
        // this is kind of work around as there is no direct support, it involves 3 roundtrips to phoenix/hbase.
        // SEQUENCE can be used for such columns in UPSERT queries
        // create sequence for each namespace and insert it into with a value uuid.
        // get the id for inserted uuid.
        // delete that entry from the table.
        long nextId = 0;
        UUID uuid = UUID.randomUUID();
        String upsertQuery = "UPSERT VALUES INTO "+namespace+"_sequence_table(id, value) VALUES( NEXT VALUE FOR "+namespace+"_sequence, "+uuid+")";
        String selectQuery = "SELECT id FROM "+namespace+"_sequence_table WHERE value='"+uuid+"'";
        String deleteQuery = "DELETE FROM "+namespace+"_sequence_table WHERE value='"+uuid+"'";

        return 0L;
    }
}
