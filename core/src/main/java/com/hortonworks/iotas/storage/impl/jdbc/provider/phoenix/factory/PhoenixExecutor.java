package com.hortonworks.iotas.storage.impl.jdbc.provider.phoenix.factory;

import com.google.common.cache.CacheBuilder;
import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.StorableKey;
import com.hortonworks.iotas.storage.impl.jdbc.config.ExecutionConfig;
import com.hortonworks.iotas.storage.impl.jdbc.connection.ConnectionBuilder;
import com.hortonworks.iotas.storage.impl.jdbc.provider.phoenix.query.PhoenixDeleteQuery;
import com.hortonworks.iotas.storage.impl.jdbc.provider.phoenix.query.PhoenixNextIdQuery;
import com.hortonworks.iotas.storage.impl.jdbc.provider.phoenix.query.PhoenixSelectQuery;
import com.hortonworks.iotas.storage.impl.jdbc.provider.phoenix.query.PhoenixUpsertQuery;
import com.hortonworks.iotas.storage.impl.jdbc.provider.sql.factory.AbstractQueryExecutor;
import com.hortonworks.iotas.storage.impl.jdbc.provider.sql.query.SqlQuery;
import com.hortonworks.iotas.storage.impl.jdbc.provider.sql.statement.PreparedStatementBuilder;

import java.util.Collection;

/**
 * SQL query executor for Phoenix
 */
public class PhoenixExecutor extends AbstractQueryExecutor {

    public PhoenixExecutor(ExecutionConfig config, ConnectionBuilder connectionBuilder) {
        super(config, connectionBuilder);
    }

    public PhoenixExecutor(ExecutionConfig config, ConnectionBuilder connectionBuilder, CacheBuilder<SqlQuery, PreparedStatementBuilder> cacheBuilder) {
        super(config, connectionBuilder, cacheBuilder);
    }

    @Override
    public void insert(Storable storable) {
        insertOrUpdate(storable);
    }

    @Override
    public void insertOrUpdate(Storable storable) {
        executeUpdate(new PhoenixUpsertQuery(storable));
    }

    @Override
    public <T extends Storable> Collection<T> select(String namespace) {
        return executeQuery(namespace, new PhoenixSelectQuery(namespace));
    }

    @Override
    public <T extends Storable> Collection<T> select(StorableKey storableKey) {
        return executeQuery(storableKey.getNameSpace(), new PhoenixSelectQuery(storableKey));
    }

    @Override
    public void delete(StorableKey storableKey) {
        executeUpdate(new PhoenixDeleteQuery(storableKey));
    }

    @Override
    public Long nextId(String namespace) {
        PhoenixNextIdQuery phoenixNextIdQuery = new PhoenixNextIdQuery(namespace, connectionBuilder, queryTimeoutSecs);
        return phoenixNextIdQuery.getNextID();
    }

}
