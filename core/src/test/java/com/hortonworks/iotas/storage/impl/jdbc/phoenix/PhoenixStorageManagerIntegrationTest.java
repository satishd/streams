package com.hortonworks.iotas.storage.impl.jdbc.phoenix;

import com.hortonworks.iotas.IntegrationTest;
import com.hortonworks.iotas.storage.impl.jdbc.JdbcStorageManager;
import com.hortonworks.iotas.storage.impl.jdbc.JdbcStorageManagerIntegrationTest;
import com.hortonworks.iotas.storage.impl.jdbc.config.ExecutionConfig;
import com.hortonworks.iotas.storage.impl.jdbc.connection.HikariCPConnectionBuilder;
import com.hortonworks.iotas.storage.impl.jdbc.phoenix.factory.PhoenixExecutor;
import com.zaxxer.hikari.HikariConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;

/**
 *
 */

@Category(IntegrationTest.class)
// ignoring for now. need to add a separate group which require HBase with phoenix to be running
// This test is running fine when hbase is running
@Ignore
public class PhoenixStorageManagerIntegrationTest extends JdbcStorageManagerIntegrationTest
{
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

}
