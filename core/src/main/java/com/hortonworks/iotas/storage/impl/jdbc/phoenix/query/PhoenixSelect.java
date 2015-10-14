package com.hortonworks.iotas.storage.impl.jdbc.phoenix.query;

import com.hortonworks.iotas.storage.StorableKey;
import com.hortonworks.iotas.storage.impl.jdbc.provider.query.ProviderStorableKeyQuery;

/**
 *
 */
public class PhoenixSelect extends ProviderStorableKeyQuery {

    public PhoenixSelect(String nameSpace) {
        super(nameSpace);
    }

    public PhoenixSelect(StorableKey storableKey) {
        super(storableKey);
    }

    // "SELECT * FROM DB.TABLE [WHERE C1 = ?, C2 = ?]"
    @Override
    protected void setParameterizedSql() {
        sql = "SELECT * FROM " + tableName;
        //where clause is defined by columns specified in the PrimaryKey
        if (columns != null) {
            sql += " WHERE " + join(getColumnNames(columns, "\"%s\" = ?"), " AND ");
        }
        log.debug(sql);
    }
}
