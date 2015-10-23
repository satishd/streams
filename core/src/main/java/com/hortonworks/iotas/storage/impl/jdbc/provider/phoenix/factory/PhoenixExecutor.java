package com.hortonworks.iotas.storage.impl.jdbc.provider.phoenix.factory;

import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.StorableKey;
import com.hortonworks.iotas.storage.exception.NonIncrementalColumnException;
import com.hortonworks.iotas.storage.impl.jdbc.config.ExecutionConfig;
import com.hortonworks.iotas.storage.impl.jdbc.connection.ConnectionBuilder;
import com.hortonworks.iotas.storage.impl.jdbc.provider.phoenix.query.PhoenixDelete;
import com.hortonworks.iotas.storage.impl.jdbc.provider.phoenix.query.PhoenixSelect;
import com.hortonworks.iotas.storage.impl.jdbc.provider.phoenix.query.PhoenixUpsert;
import com.hortonworks.iotas.storage.impl.jdbc.provider.sql.factory.ProviderQueryExecutor;

import java.util.Collection;

/**
 *
 */
public class PhoenixExecutor extends ProviderQueryExecutor {

    public PhoenixExecutor(ExecutionConfig config, ConnectionBuilder connectionBuilder) {
        super(config, connectionBuilder);
    }

    @Override
    public void insert(Storable storable) {
        insertOrUpdate(storable);
    }

    @Override
    public void insertOrUpdate(Storable storable) {
        executeUpdate(new PhoenixUpsert(storable));
    }

    @Override
    public <T extends Storable> Collection<T> select(String namespace) {
        return executeQuery(namespace, new PhoenixSelect(namespace));
    }

    @Override
    public <T extends Storable> Collection<T> select(StorableKey storableKey) {
        return executeQuery(storableKey.getNameSpace(), new PhoenixSelect(storableKey));
    }

    @Override
    public void delete(StorableKey storableKey) {
        executeUpdate(new PhoenixDelete(storableKey));
    }

    @Override
    public Long nextId(String namespace) {
        // SEQUENCE can be used for such columns in UPSERT queries
        throw new NonIncrementalColumnException("Phoenix does not support auto increment columns");
    }

}
