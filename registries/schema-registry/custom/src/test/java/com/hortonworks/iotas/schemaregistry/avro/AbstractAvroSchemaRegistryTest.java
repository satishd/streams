package com.hortonworks.iotas.schemaregistry.avro;


import com.hortonworks.iotas.schemaregistry.SchemaInfo;
import com.hortonworks.iotas.schemaregistry.SchemaProvider;
import org.apache.avro.Schema;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.io.InputStream;

/**
 *
 */
public abstract class AbstractAvroSchemaRegistryTest {

    protected String schema1;
    protected String schema2;

    protected void setup() throws IOException {
        schema1 = getSchema("/device.avsc");
        schema2 = getSchema("/device2.avsc");

    }

    private String getSchema(String schemaFileName) throws IOException {
        InputStream avroSchemaStream = AvroSerDeTest.class.getResourceAsStream(schemaFileName);
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(avroSchemaStream).toString();
    }

    @Test
    public void testRegistryOps() {
        String type = AvroSchemaProvider.TYPE;

        SchemaInfo schemaInfo = new SchemaInfo("schema-"+System.currentTimeMillis(), type);
        schemaInfo.setCompatibility(SchemaProvider.Compatibility.BOTH);
        schemaInfo.setSchemaText(schema1);

        addSchemaAndVerify(schemaInfo);

        schemaInfo.setSchemaText(schema2);
        addSchemaAndVerify(schemaInfo);

        SchemaInfo latest = getLatestSchema(type, schemaInfo.getName());
        Assert.assertEquals(latest, schemaInfo);

        testCompatibility(type, schema1, schema2);
    }

    private void addSchemaAndVerify(SchemaInfo schemaInfo) {
        SchemaInfo addedSchemaInfo = addSchema(schemaInfo);
        Integer nextVersion = schemaInfo.getVersion() == null ? 0 : schemaInfo.getVersion();
        schemaInfo.setVersion(nextVersion+1);
        schemaInfo.setId(addedSchemaInfo.getId());
        schemaInfo.setTimestamp(addedSchemaInfo.getTimestamp());
        Assert.assertEquals(addedSchemaInfo, schemaInfo);
    }

    protected abstract SchemaInfo getLatestSchema(String type, String name);

    protected abstract SchemaInfo addSchema(SchemaInfo schemaInfo);

    protected abstract void testCompatibility(String type, String schema1, String schema2);

}