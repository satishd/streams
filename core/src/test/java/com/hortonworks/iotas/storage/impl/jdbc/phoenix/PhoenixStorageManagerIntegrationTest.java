package com.hortonworks.iotas.storage.impl.jdbc.phoenix;

import com.hortonworks.iotas.storage.impl.jdbc.JdbcStorageManager;
import com.hortonworks.iotas.storage.impl.jdbc.JdbcStorageManagerIntegrationTest;
import com.hortonworks.iotas.storage.impl.jdbc.config.ExecutionConfig;
import com.hortonworks.iotas.storage.impl.jdbc.connection.HikariCPConnectionBuilder;
import com.hortonworks.iotas.storage.impl.jdbc.phoenix.factory.PhoenixExecutor;
import com.hortonworks.iotas.test.HBaseIntegrationTest;
import com.zaxxer.hikari.HikariConfig;
import jdk.nashorn.internal.ir.annotations.Ignore;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;

/**
 *
 */

@Category(HBaseIntegrationTest.class)
public class PhoenixStorageManagerIntegrationTest extends JdbcStorageManagerIntegrationTest {
    @Before
    public void setUp() throws Exception {
        PhoenixClient phoenixClient = new PhoenixClient();
        phoenixClient.runScript("phoenix/create_tables.sql");
    }

    @After
    public void tearDown() throws Exception {
        PhoenixClient phoenixClient = new PhoenixClient();
        phoenixClient.runScript("phoenix/drop_tables.sql");
        jdbcStorageManager.cleanup();
    }

    @BeforeClass
    public static void setUpClass() throws Exception {
        HikariConfig hikariConfig = new HikariConfig();
        Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
        hikariConfig.setJdbcUrl("jdbc:phoenix:localhost:2181");

        JdbcStorageManagerIntegrationTest.connectionBuilder = new HikariCPConnectionBuilder(hikariConfig);
        jdbcStorageManager = new JdbcStorageManager(new PhoenixExecutor(new ExecutionConfig(-1), connectionBuilder));
        database = Database.PHOENIX;
    }

    @Ignore // ignore this test as phoenix does not support auto increment columns
    public void testNextId_AutoincrementColumn_IdPlusOne() throws Exception {
        
    }

}