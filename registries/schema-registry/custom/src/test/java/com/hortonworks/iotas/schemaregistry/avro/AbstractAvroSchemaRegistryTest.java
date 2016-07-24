package com.hortonworks.iotas.schemaregistry.avro;


import com.hortonworks.iotas.schemaregistry.SchemaInfo;
import com.hortonworks.iotas.schemaregistry.SchemaNotFoundException;
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
    protected String schemaName;

    protected void setup() throws IOException {
        schema1 = getSchema("/device.avsc");
        schema2 = getSchema("/device2.avsc");
        schemaName = "schema-"+System.currentTimeMillis();
    }

    private String getSchema(String schemaFileName) throws IOException {
        InputStream avroSchemaStream = AvroSerDeTest.class.getResourceAsStream(schemaFileName);
        Schema.Parser parser = new Schema.Parser();
        return parser.parse(avroSchemaStream).toString();
    }

    @Test
    public void testRegistryOps() throws Exception {
        String type = AvroSchemaProvider.TYPE;

        SchemaInfo schemaInfo1 = new SchemaInfo(schemaName, type);
        schemaInfo1.setCompatibility(SchemaProvider.Compatibility.BOTH);
        schemaInfo1.setSchemaText(schema1);
        int v1 = addSchemaAndVerify(schemaInfo1).getVersion();

        SchemaInfo schemaInfo2 = new SchemaInfo(schemaName, type);
        schemaInfo2.setCompatibility(SchemaProvider.Compatibility.BOTH);
        schemaInfo2.setSchemaText(schema2);
        SchemaInfo addedSchemaInfo2 = addSchemaAndVerify(schemaInfo2);
        int v2 = addedSchemaInfo2.getVersion();

        Assert.assertTrue(v2 == v1+1);

        SchemaInfo latest = getLatestSchema(type, schemaName);
        Assert.assertEquals(latest, addedSchemaInfo2);

        testCompatibility(type, v1, v2);
    }

    private SchemaInfo addSchemaAndVerify(SchemaInfo schemaInfo) {
        SchemaInfo addedSchemaInfo = addSchema(schemaInfo);
        Assert.assertEquals(addedSchemaInfo.getName(), schemaInfo.getName());
        Assert.assertEquals(addedSchemaInfo.getSchemaText(), schemaInfo.getSchemaText());
        return addedSchemaInfo;
    }

    protected abstract SchemaInfo getLatestSchema(String type, String name);

    protected abstract SchemaInfo addSchema(SchemaInfo schemaInfo);

    protected abstract void testCompatibility(String type, int version1, int version2) throws Exception;

}