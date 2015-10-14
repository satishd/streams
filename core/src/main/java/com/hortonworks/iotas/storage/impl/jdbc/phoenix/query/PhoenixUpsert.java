package com.hortonworks.iotas.storage.impl.jdbc.phoenix.query;

import com.hortonworks.iotas.storage.Storable;
import com.hortonworks.iotas.storage.impl.jdbc.provider.query.ProviderStorableSqlQuery;

/**
 *
 */
public class PhoenixUpsert extends ProviderStorableSqlQuery {

    public PhoenixUpsert(Storable storable) {
        super(storable);
    }

    @Override
    protected void setParameterizedSql() {
        sql = "UPSERT INTO " + tableName + " ("
                + join(getColumnNames(columns, "\"%s\""), ", ")
                + ") VALUES( " + getBindVariables("?,", columns.size()) + ")";
        log.debug(sql);
        System.out.println("###################################### sql = \n" + sql);
    }
}
