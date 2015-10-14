package com.hortonworks.iotas.storage.impl.jdbc.phoenix.query;

import com.hortonworks.iotas.storage.StorableKey;
import com.hortonworks.iotas.storage.impl.jdbc.provider.query.ProviderStorableKeyQuery;

/**
 *
 */
public class PhoenixDelete extends ProviderStorableKeyQuery {

    public PhoenixDelete(String nameSpace) {
        super(nameSpace);
    }

    public PhoenixDelete(StorableKey storableKey) {
        super(storableKey);
    }

    @Override
    protected void setParameterizedSql() {
        sql = "DELETE FROM  " + tableName + " WHERE " + join(getColumnNames(columns, "\"%s\" = ?"), " AND ");
        log.debug(sql);
    }
}
