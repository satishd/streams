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

import com.hortonworks.iotas.storage.impl.jdbc.config.ExecutionConfig;
import com.hortonworks.iotas.storage.impl.jdbc.connection.ConnectionBuilder;
import com.hortonworks.iotas.storage.impl.jdbc.provider.mysql.query.MySqlQuery;
import com.hortonworks.iotas.storage.impl.jdbc.provider.sql.query.AbstractSqlQuery;
import com.hortonworks.iotas.storage.impl.jdbc.provider.sql.statement.PreparedStatementBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

/**
 * Query to get next sequence id in phoenix for a given name space.
 */
public class PhoenixNextIdQuery {

    private static final Logger logger = LoggerFactory.getLogger(PhoenixNextIdQuery.class);
    private String namespace;
    private final ConnectionBuilder connectionBuilder;
    private final int queryTimeoutSecs;

    public PhoenixNextIdQuery(String namespace, ConnectionBuilder connectionBuilder, int queryTimeoutSecs) {
        this.namespace = namespace;
        this.connectionBuilder = connectionBuilder;
        this.queryTimeoutSecs = queryTimeoutSecs;
    }

    public Long getNextID() {
        // this is kind of work around as there is no direct support, it involves 3 roundtrips to phoenix/hbase (inefficient!!).
        // SEQUENCE can be used for such columns in UPSERT queries directly but to get a simple sequence-id involves all this.
        // create sequence for each namespace and insert it into with a value uuid.
        // get the id for inserted uuid.
        // delete that entry from the table.
        long nextId = 0;
        UUID uuid = UUID.randomUUID();
        PhoenixSqlQuery updateQuery =new PhoenixSqlQuery("UPSERT INTO "+namespace+"_sequence_table(\"id\", \"value\") VALUES( NEXT VALUE FOR "+namespace+"_sequence, '"+uuid+"')");
        PhoenixSqlQuery selectQuery = new PhoenixSqlQuery("SELECT \"id\" FROM "+namespace+"_sequence_table WHERE \"value\"='"+uuid+"'");
        PhoenixSqlQuery deleteQuery = new PhoenixSqlQuery("DELETE FROM "+namespace+"_sequence_table WHERE \"value\"='"+uuid+"'");

        try (Connection connection = connectionBuilder.getConnection();) {
            int x = new PreparedStatementBuilder(connection, new ExecutionConfig(queryTimeoutSecs), updateQuery).getPreparedStatement(updateQuery).executeUpdate();
            System.out.println("####### x = " + x);
            ResultSet selectResultSet =  new PreparedStatementBuilder(connection, new ExecutionConfig(queryTimeoutSecs), selectQuery).getPreparedStatement(selectQuery).executeQuery();
            if(selectResultSet.next()) {
                nextId = selectResultSet.getLong("id");
            } else {
                throw new RuntimeException("No id created for the current sequence");
            }
            System.out.println("####### id = " + nextId);
            int y =  new PreparedStatementBuilder(connection, new ExecutionConfig(queryTimeoutSecs), deleteQuery).getPreparedStatement(deleteQuery).executeUpdate();
            System.out.println("####### y = " + y);
        } catch (SQLException e) {
            logger.error(e.getMessage(), e);
        }

        return nextId;
    }

    static class PhoenixSqlQuery extends AbstractSqlQuery {

        public PhoenixSqlQuery(String sql) {
            super();
            this.sql = sql;
        }

        @Override
        protected void setParameterizedSql() {
        }
    }
}
