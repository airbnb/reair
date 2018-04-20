package test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.common.HiveParameterKeys;
import com.airbnb.reair.db.DbConnectionFactory;
import com.airbnb.reair.db.DbKeyValueStore;
import com.airbnb.reair.db.EmbeddedMySqlDb;
import com.airbnb.reair.db.StaticDbConnectionFactory;
import com.airbnb.reair.db.TestDbCredentials;
import com.airbnb.reair.hive.hooks.AuditLogHookUtils;
import com.airbnb.reair.hive.hooks.CliAuditLogHook;
import com.airbnb.reair.hive.hooks.HiveOperation;
import com.airbnb.reair.incremental.DirectoryCopier;
import com.airbnb.reair.incremental.ReplicationServer;
import com.airbnb.reair.incremental.auditlog.AuditLogReader;
import com.airbnb.reair.incremental.db.PersistedJobInfoStore;
import com.airbnb.reair.incremental.filter.PassThoughReplicationFilter;
import com.airbnb.reair.incremental.filter.ReplicationFilter;
import com.airbnb.reair.utils.ReplicationTestUtils;

import com.timgroup.statsd.NoOpStatsDClient;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;

public class ReplicationServerTest extends MockClusterTest {

  private static final Log LOG = LogFactory.getLog(
      ReplicationServerTest.class);

  private static EmbeddedMySqlDb embeddedMySqlDb;

  private static final String AUDIT_LOG_DB_NAME = "audit_log_db";
  private static final String AUDIT_LOG_TABLE_NAME = "audit_log";
  private static final String AUDIT_LOG_OBJECTS_TABLE_NAME = "audit_objects";
  private static final String AUDIT_LOG_MAP_RED_STATS_TABLE_NAME =
      "mapred_stats";

  private static final String REPLICATION_STATE_DB_NAME =
      "replication_state_db";
  private static final String KEY_VALUE_TABLE_NAME = "key_value";
  private static final String REPLICATION_JOB_STATE_TABLE_NAME =
      "replication_state";

  private static final String HIVE_DB = "test_db";

  // To speed up execution of the test, specify this poll interval for the
  // replication server
  private static final long TEST_POLL_TIME = 500;

  private static CliAuditLogHook cliAuditLogHook;
  private static AuditLogReader auditLogReader;
  private static DbKeyValueStore dbKeyValueStore;
  private static PersistedJobInfoStore persistedJobInfoStore;
  private static ReplicationFilter replicationFilter;

  /**
   * Sets up this class for testing.
   *
   * @throws IOException if there's an error accessing the local filesystem
   * @throws SQLException if there's an error querying the DB
   */
  @BeforeClass
  public static void setupClass() throws IOException, SQLException {
    MockClusterTest.setupClass();
    embeddedMySqlDb = new EmbeddedMySqlDb();
    embeddedMySqlDb.startDb();

    resetState();
  }

  private static void resetState() throws IOException, SQLException {
    TestDbCredentials testDbCredentials = new TestDbCredentials();
    DbConnectionFactory dbConnectionFactory = new StaticDbConnectionFactory(
        ReplicationTestUtils.getJdbcUrl(embeddedMySqlDb),
        testDbCredentials.getReadWriteUsername(),
        testDbCredentials.getReadWritePassword());

    // Drop the databases to start fresh
    ReplicationTestUtils.dropDatabase(dbConnectionFactory,
        AUDIT_LOG_DB_NAME);
    ReplicationTestUtils.dropDatabase(dbConnectionFactory,
        REPLICATION_STATE_DB_NAME);

    // Create the audit log DB and tables
    AuditLogHookUtils.setupAuditLogTables(dbConnectionFactory,
        AUDIT_LOG_DB_NAME,
        AUDIT_LOG_TABLE_NAME,
        AUDIT_LOG_OBJECTS_TABLE_NAME,
        AUDIT_LOG_MAP_RED_STATS_TABLE_NAME);

    // Recreate the connection factory so that it uses the database
    DbConnectionFactory auditLogDbConnectionFactory =
        new StaticDbConnectionFactory(
            ReplicationTestUtils.getJdbcUrl(embeddedMySqlDb,
              AUDIT_LOG_DB_NAME),
            testDbCredentials.getReadWriteUsername(),
            testDbCredentials.getReadWritePassword());

    cliAuditLogHook = new CliAuditLogHook(testDbCredentials);

    // Setup the DB and tables needed to store replication state
    setupReplicationServerStateTables(dbConnectionFactory,
        REPLICATION_STATE_DB_NAME,
        KEY_VALUE_TABLE_NAME,
        REPLICATION_JOB_STATE_TABLE_NAME);

    DbConnectionFactory replicationStateDbConnectionFactory =
        new StaticDbConnectionFactory(
            ReplicationTestUtils.getJdbcUrl(embeddedMySqlDb,
              REPLICATION_STATE_DB_NAME),
            testDbCredentials.getReadWriteUsername(),
            testDbCredentials.getReadWritePassword());

    auditLogReader = new AuditLogReader(
        conf,
        auditLogDbConnectionFactory,
        AUDIT_LOG_TABLE_NAME,
        AUDIT_LOG_OBJECTS_TABLE_NAME,
        AUDIT_LOG_MAP_RED_STATS_TABLE_NAME,
        0);

    dbKeyValueStore = new DbKeyValueStore(
        replicationStateDbConnectionFactory,
        KEY_VALUE_TABLE_NAME);

    persistedJobInfoStore =
        new PersistedJobInfoStore(
            conf,
            replicationStateDbConnectionFactory,
            REPLICATION_JOB_STATE_TABLE_NAME);

    replicationFilter = new PassThoughReplicationFilter();
    replicationFilter.setConf(conf);
  }

  private static void clearMetastores() throws HiveMetastoreException {
    // Drop all tables from the destination metastore
    for (String tableName : srcMetastore.getTables(HIVE_DB, "*")) {
      srcMetastore.dropTable(HIVE_DB, tableName, true);
    }
    // Drop all partitions from the destination metastore
    for (String tableName : destMetastore.getTables(HIVE_DB, "*")) {
      destMetastore.dropTable(HIVE_DB, tableName, true);
    }
  }

  private static void setupReplicationServerStateTables(
      DbConnectionFactory dbConnectionFactory,
      String dbName,
      String keyValueTableName,
      String persistedJobInfoTableName) throws SQLException {
    String createDbSql = String.format("CREATE DATABASE %s", dbName);
    String createKeyValueTableSql =
        DbKeyValueStore.getCreateTableSql(keyValueTableName);
    String createPersistedJobInfoTable =
        PersistedJobInfoStore.getCreateTableSql(
            persistedJobInfoTableName);

    Connection connection = dbConnectionFactory.getConnection();

    Statement statement = connection.createStatement();

    // Create the tables
    try {
      statement.execute(createDbSql);

      connection.setCatalog(dbName);

      statement = connection.createStatement();
      statement.execute(createKeyValueTableSql);
      statement.execute(createPersistedJobInfoTable);
    } finally {
      statement.close();
      connection.close();
    }
  }

  @Test
  public void testTableReplication() throws Exception {
    // Reset the state
    resetState();
    clearMetastores();

    // Create an unpartitioned table in the source and a corresponding
    // entry in the audit log
    String dbName = "test_db";
    String tableName = "test_table";
    simulatedCreateUnpartitionedTable(dbName, tableName);

    // Have the replication server copy it.
    ReplicationServer replicationServer = createReplicationServer();
    replicationServer.run(1);

    // Verify that the object was copied
    assertTrue(destMetastore.existsTable(dbName, tableName));
  }

  @Test
  public void testPartitionReplication() throws Exception {
    // Reset the state
    resetState();
    clearMetastores();

    // Create an partitioned table in the source and a corresponding
    // entry in the audit log
    String dbName = "test_db";
    String tableName = "test_table";
    String partitionName = "ds=1/hr=2";

    simulateCreatePartitionedTable(dbName, tableName);
    simulateCreatePartition(dbName, tableName, partitionName);

    // Have the replication server copy it.
    ReplicationServer replicationServer = createReplicationServer();

    replicationServer.run(2);

    // Verify that the object was copied
    assertTrue(destMetastore.existsTable(dbName, tableName));
    assertTrue(destMetastore.existsPartition(
          dbName,
          tableName,
          partitionName));
  }

  @Test
  public void testOptimizedPartitionReplication() throws Exception {
    // Reset the state
    resetState();
    clearMetastores();

    // Create an partitioned table in the source and a corresponding
    // entry in the audit log
    final String dbName = "test_db";
    final String tableName = "test_table";
    final List<String> partitionNames = new ArrayList<>();
    partitionNames.add("ds=1/hr=1");
    partitionNames.add("ds=1/hr=2");
    partitionNames.add("ds=1/hr=3");

    simulateCreatePartitionedTable(dbName, tableName);
    simulateCreatePartitions(dbName, tableName, partitionNames);

    // Have the replication server copy it.
    final ReplicationServer replicationServer = createReplicationServer();

    replicationServer.run(2);
    LOG.error("Server stopped");
    // Verify that the object was copied
    assertTrue(destMetastore.existsTable(dbName, tableName));

    for (String partitionName : partitionNames) {
      assertTrue(destMetastore.existsPartition(
            dbName,
            tableName,
            partitionName));
    }
  }

  private void removeTableAttributes(
      List<org.apache.hadoop.hive.ql.metadata.Table> tables) {
    for (org.apache.hadoop.hive.ql.metadata.Table table : tables) {
      final Table newTable = new Table(table.getTTable());
      newTable.setParameters(Collections.emptyMap());
      table.setTTable(newTable);
    }
  }

  private void removePartitionAttributes(
      List<org.apache.hadoop.hive.ql.metadata.Partition> partitions) {
    for (org.apache.hadoop.hive.ql.metadata.Partition p : partitions) {
      final Table newTable = new Table(p.getTable().getTTable());
      Partition newPartition = new Partition(p.getTPartition());
      newTable.setParameters(null);

      newPartition.setParameters(null);

      p.getTable().setTTable(newTable);
      p.setTPartition(newPartition);
    }
  }

  private void simulatedCreateUnpartitionedTable(String dbName, String tableName) throws Exception {
    // Create an unpartitioned table in the source and a corresponding entry in the audit log
    HiveObjectSpec unpartitionedTable = new HiveObjectSpec(dbName,
        tableName);
    Table srcTable = ReplicationTestUtils.createUnpartitionedTable(conf,
        srcMetastore,
        unpartitionedTable,
        TableType.MANAGED_TABLE,
        srcWarehouseRoot);

    List<org.apache.hadoop.hive.ql.metadata.Table> inputTables =
        new ArrayList<>();
    List<org.apache.hadoop.hive.ql.metadata.Table> outputTables =
        new ArrayList<>();
    outputTables.add(new org.apache.hadoop.hive.ql.metadata.Table(srcTable));
    removeTableAttributes(outputTables);

    HiveConf hiveConf = AuditLogHookUtils.getHiveConf(
        embeddedMySqlDb,
        AUDIT_LOG_DB_NAME,
        AUDIT_LOG_TABLE_NAME,
        AUDIT_LOG_OBJECTS_TABLE_NAME,
        AUDIT_LOG_MAP_RED_STATS_TABLE_NAME);
    AuditLogHookUtils.insertAuditLogEntry(
        cliAuditLogHook,
        HiveOperation.QUERY,
        "Example query string",
        inputTables,
        new ArrayList<>(),
        outputTables,
        new ArrayList<>(),
        new HashMap<>(),
        hiveConf);
  }

  private void simulatedRenameTable(String dbName,
      String oldTableName,
      String newTableName,
      boolean isThriftAuditLog)
    throws Exception {
    Table srcTable = srcMetastore.getTable(dbName, oldTableName);
    Table renamedTable = new Table(srcTable);
    renamedTable.setTableName(newTableName);
    srcMetastore.alterTable(dbName, oldTableName, renamedTable);

    List<org.apache.hadoop.hive.ql.metadata.Table> inputTables =
        new ArrayList<>();
    org.apache.hadoop.hive.ql.metadata.Table qlSrcTable =
        new org.apache.hadoop.hive.ql.metadata.Table(srcTable);
    inputTables.add(qlSrcTable);

    List<org.apache.hadoop.hive.ql.metadata.Table> outputTables =
        new ArrayList<>();
    outputTables.add(qlSrcTable);
    org.apache.hadoop.hive.ql.metadata.Table qlRenamedTable =
        new org.apache.hadoop.hive.ql.metadata.Table(renamedTable);
    outputTables.add(qlRenamedTable);

    if (isThriftAuditLog) {
      HiveConf hiveConf = AuditLogHookUtils.getMetastoreHiveConf(
          embeddedMySqlDb,
          AUDIT_LOG_DB_NAME,
          AUDIT_LOG_TABLE_NAME,
          AUDIT_LOG_OBJECTS_TABLE_NAME
      );

      AuditLogHookUtils.insertThriftRenameTableLogEntry(
          srcTable,
          renamedTable,
          hiveConf
      );
    } else {
      HiveConf hiveConf = AuditLogHookUtils.getHiveConf(
          embeddedMySqlDb,
          AUDIT_LOG_DB_NAME,
          AUDIT_LOG_TABLE_NAME,
          AUDIT_LOG_OBJECTS_TABLE_NAME,
          AUDIT_LOG_MAP_RED_STATS_TABLE_NAME);

      AuditLogHookUtils.insertAuditLogEntry(
          cliAuditLogHook,
          HiveOperation.ALTERTABLE_RENAME,
          "Example query string",
          inputTables,
          new ArrayList<>(),
          outputTables,
          new ArrayList<>(),
          new HashMap<>(),
          hiveConf);
    }
  }

  private void simulatedRenamePartition(String dbName,
      String tableName,
      String oldPartitionName,
      List<String> newPartitionValues) throws Exception {
    Partition oldPartition = srcMetastore.getPartition(dbName, tableName, oldPartitionName);
    Partition newPartition = new Partition(oldPartition);
    newPartition.setValues(newPartitionValues);

    HiveConf hiveConf = AuditLogHookUtils.getMetastoreHiveConf(
        embeddedMySqlDb,
        AUDIT_LOG_DB_NAME,
        AUDIT_LOG_TABLE_NAME,
        AUDIT_LOG_OBJECTS_TABLE_NAME
    );

    HiveMetaStore.HMSHandler handler = Mockito.mock(HiveMetaStore.HMSHandler.class);
    Mockito.when(
        handler.get_table(dbName, tableName)
    ).thenReturn(srcMetastore.getTable(dbName, tableName));

    AuditLogHookUtils.insertThriftRenamePartitionLogEntry(
        handler,
        oldPartition,
        newPartition,
        hiveConf
    );
  }

  private void simulateCreatePartitionedTable(String dbName, String tableName) throws Exception {
    // Create an unpartitioned table in the source and a corresponding entry in the audit log
    HiveObjectSpec unpartitionedTable = new HiveObjectSpec(dbName,
        tableName);
    Table srcTable = ReplicationTestUtils.createPartitionedTable(conf,
        srcMetastore,
        unpartitionedTable,
        TableType.MANAGED_TABLE,
        srcWarehouseRoot);

    List<org.apache.hadoop.hive.ql.metadata.Table> inputTables =
        new ArrayList<>();
    List<org.apache.hadoop.hive.ql.metadata.Table> outputTables =
        new ArrayList<>();
    outputTables.add(new org.apache.hadoop.hive.ql.metadata.Table(srcTable));
    removeTableAttributes(outputTables);

    HiveConf hiveConf = AuditLogHookUtils.getHiveConf(
        embeddedMySqlDb,
        AUDIT_LOG_DB_NAME,
        AUDIT_LOG_TABLE_NAME,
        AUDIT_LOG_OBJECTS_TABLE_NAME,
        AUDIT_LOG_MAP_RED_STATS_TABLE_NAME);
    AuditLogHookUtils.insertAuditLogEntry(
        cliAuditLogHook,
        HiveOperation.QUERY,
        "Example query string",
        inputTables,
        new ArrayList<>(),
        outputTables,
        new ArrayList<>(),
        new HashMap<>(),
        hiveConf);

  }

  private void simulateCreatePartition(String dbName,
      String tableName,
      String partitionName)
    throws Exception {
    HiveObjectSpec partitionSpec = new HiveObjectSpec(dbName, tableName,
        partitionName);

    Table srcTable = srcMetastore.getTable(dbName, tableName);
    Partition srcPartition = ReplicationTestUtils.createPartition(conf,
        srcMetastore,
        partitionSpec);

    List<org.apache.hadoop.hive.ql.metadata.Table> inputTables =
        new ArrayList<>();
    List<org.apache.hadoop.hive.ql.metadata.Partition> outputPartitions =
        new ArrayList<>();

    inputTables.add(new org.apache.hadoop.hive.ql.metadata.Table(srcTable));
    outputPartitions.add(new org.apache.hadoop.hive.ql.metadata.Partition(
          new org.apache.hadoop.hive.ql.metadata.Table(srcTable),
          srcPartition));

    removeTableAttributes(inputTables);
    removePartitionAttributes(outputPartitions);

    HiveConf hiveConf = AuditLogHookUtils.getHiveConf(
        embeddedMySqlDb,
        AUDIT_LOG_DB_NAME,
        AUDIT_LOG_TABLE_NAME,
        AUDIT_LOG_OBJECTS_TABLE_NAME,
        AUDIT_LOG_MAP_RED_STATS_TABLE_NAME);
    AuditLogHookUtils.insertAuditLogEntry(
        cliAuditLogHook,
        HiveOperation.QUERY,
        "Example query string",
        inputTables,
        new ArrayList<>(),
        new ArrayList<>(),
        outputPartitions,
        new HashMap<>(),
        hiveConf);
  }

  private void simulateCreatePartitions(String dbName,
      String tableName,
      List<String> partitionNames)
    throws Exception {
    List<org.apache.hadoop.hive.ql.metadata.Table> inputTables =
        new ArrayList<>();
    List<org.apache.hadoop.hive.ql.metadata.Partition> outputPartitions =
        new ArrayList<>();


    Table srcTable = srcMetastore.getTable(dbName, tableName);
    inputTables.add(new org.apache.hadoop.hive.ql.metadata.Table(srcTable));

    for (String partitionName : partitionNames) {
      HiveObjectSpec partitionSpec = new HiveObjectSpec(dbName, tableName,
          partitionName);

      Partition srcPartition = ReplicationTestUtils.createPartition(conf,
          srcMetastore,
          partitionSpec);

      outputPartitions.add(new org.apache.hadoop.hive.ql.metadata.Partition(
            new org.apache.hadoop.hive.ql.metadata.Table(srcTable),
            srcPartition));
    }

    removeTableAttributes(inputTables);
    removePartitionAttributes(outputPartitions);

    HiveConf hiveConf = AuditLogHookUtils.getHiveConf(
        embeddedMySqlDb,
        AUDIT_LOG_DB_NAME,
        AUDIT_LOG_TABLE_NAME,
        AUDIT_LOG_OBJECTS_TABLE_NAME,
        AUDIT_LOG_MAP_RED_STATS_TABLE_NAME);
    AuditLogHookUtils.insertAuditLogEntry(
        cliAuditLogHook,
        HiveOperation.QUERY,
        "Example query string",
        inputTables,
        new ArrayList<>(),
        new ArrayList<>(),
        outputPartitions,
        new HashMap<>(),
        hiveConf);
  }


  private void simulateDropTable(String dbName, String tableName) throws Exception {
    // Drop the specified table from the source and also generate the appropriate audit log entry
    Table srcTable = srcMetastore.getTable(dbName, tableName);
    srcMetastore.dropTable(dbName, tableName, false);

    List<org.apache.hadoop.hive.ql.metadata.Table> outputTables = new ArrayList<>();
    outputTables.add(new org.apache.hadoop.hive.ql.metadata.Table(srcTable));

    HiveConf hiveConf = AuditLogHookUtils.getHiveConf(
        embeddedMySqlDb,
        AUDIT_LOG_DB_NAME,
        AUDIT_LOG_TABLE_NAME,
        AUDIT_LOG_OBJECTS_TABLE_NAME,
        AUDIT_LOG_MAP_RED_STATS_TABLE_NAME);
    AuditLogHookUtils.insertAuditLogEntry(
        cliAuditLogHook,
        HiveOperation.DROPTABLE,
        "Example query string",
        new ArrayList<>(),
        new ArrayList<>(),
        outputTables,
        new ArrayList<>(),
        new HashMap<>(),
        hiveConf);
  }

  private void simulateDropPartition(String dbName, String tableName, String partitionName)
      throws Exception {
    // Drop the specified partition from the source and also generate the
    // appropriate audit log entry
    Table srcTable = srcMetastore.getTable(dbName, tableName);
    Partition srcPartition = srcMetastore.getPartition(dbName, tableName,
        partitionName);
    srcMetastore.dropPartition(dbName, tableName, partitionName, false);

    List<org.apache.hadoop.hive.ql.metadata.Table> inputTables =
        new ArrayList<>();
    org.apache.hadoop.hive.ql.metadata.Table qlTable =
        new org.apache.hadoop.hive.ql.metadata.Table(srcTable);
    inputTables.add(qlTable);

    List<org.apache.hadoop.hive.ql.metadata.Partition> outputPartitions =
        new ArrayList<>();
    outputPartitions.add(
        new org.apache.hadoop.hive.ql.metadata.Partition(qlTable,
          srcPartition));

    HiveConf hiveConf = AuditLogHookUtils.getHiveConf(
        embeddedMySqlDb,
        AUDIT_LOG_DB_NAME,
        AUDIT_LOG_TABLE_NAME,
        AUDIT_LOG_OBJECTS_TABLE_NAME,
        AUDIT_LOG_MAP_RED_STATS_TABLE_NAME);
    AuditLogHookUtils.insertAuditLogEntry(
        cliAuditLogHook,
        HiveOperation.ALTERTABLE_DROPPARTS,
        "Example query string",
        inputTables,
        new ArrayList<>(),
        new ArrayList<>(),
        outputPartitions,
        new HashMap<>(),
        hiveConf);
  }

  /**
   * Converts a partition name into a spec used for DDL commands. For example,
   * ds=1/hr=2 -> PARTITION(ds='1', hr='2'). Note the special characters are
   * not escapsed as they are in production.
   */
  public static String partitionNameToDdlSpec(String partitionName) {

    String[] partitionNameSplit = partitionName.split("/");
    List<String> columnExpressions = new ArrayList<>();

    for (String columnValue : partitionNameSplit) {
      String[] columnValueSplit = columnValue.split("=");
      if (columnValueSplit.length != 2) {
        throw new RuntimeException("Invalid partition name "
            + partitionName);
      }
      columnExpressions.add(columnValueSplit[0] + "='"
          + columnValueSplit[1] + "'");
    }
    return "PARTITION(" + StringUtils.join(columnExpressions, ", ") + ")";
  }


  private void simulateExchangePartition(String exchangeFromDbName,
      String exchangeFromTableName,
      String exchangeToDbName,
      String exchangeToTableName,
      String partitionName)
    throws Exception {

    // Do the exchange
    srcMetastore.exchangePartition(
        srcMetastore.partitionNameToMap(partitionName),
        exchangeFromDbName,
        exchangeFromTableName,
        exchangeToDbName,
        exchangeToTableName);

    String query = String.format("ALTER TABLE %s.%s EXCHANGE "
        + "%s WITH TABLE %s.%s",
        exchangeToDbName,
        exchangeToTableName,
        partitionNameToDdlSpec(partitionName),
        exchangeFromDbName,
        exchangeFromTableName);

    // Generate the broken audit log entry. Hive should be fixed to have the
    // correct entry. It's broken in that the command type is null and
    // inputs and outputs are empty
    HiveConf hiveConf = AuditLogHookUtils.getHiveConf(
        embeddedMySqlDb,
        AUDIT_LOG_DB_NAME,
        AUDIT_LOG_TABLE_NAME,
        AUDIT_LOG_OBJECTS_TABLE_NAME,
        AUDIT_LOG_MAP_RED_STATS_TABLE_NAME);
    AuditLogHookUtils.insertAuditLogEntry(
        cliAuditLogHook,
        null,
        query,
        new ArrayList<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        new ArrayList<>(),
        new HashMap<>(),
        hiveConf);
  }

  private ReplicationServer createReplicationServer() {
    ReplicationServer replicationServer = new ReplicationServer(
        conf,
        srcCluster,
        destCluster,
        auditLogReader,
        dbKeyValueStore,
        persistedJobInfoStore,
        Arrays.asList(replicationFilter),
        new DirectoryCopier(conf, srcCluster.getTmpDir(), false),
        new NoOpStatsDClient(),
        1,
        2,
        Optional.of(0L));
    replicationServer.setPollWaitTimeMs(TEST_POLL_TIME);
    return replicationServer;
  }

  /**
   * Tests to make sure that entries that were not completed in the previous
   * invocation of the server are picked up and run on a subsequent
   * invocation.
   *
   * @throws Exception if there is an error setting up or running this test
   */
  @Test
  public void testResumeJobs() throws Exception {
    // Reset the state
    resetState();
    clearMetastores();

    String dbName = "test_db";
    String firstTableName = "test_table_1";
    String secondTableName = "test_table_2";

    // Create the objects and the audit log entry
    simulatedCreateUnpartitionedTable(dbName, firstTableName);
    simulatedCreateUnpartitionedTable(dbName, secondTableName);

    // Have the replication server copy the first table
    ReplicationServer replicationServer = createReplicationServer();
    replicationServer.run(1);

    // Verify that the object was copied
    assertTrue(destMetastore.existsTable(dbName, firstTableName));

    assertFalse(destMetastore.existsTable(dbName, secondTableName));

    // Re-run. Since the last run finished the first entry, the second run
    // should copy the second entry.
    replicationServer.run(1);

    // Verify that the second object was copied
    assertTrue(destMetastore.existsTable(dbName, firstTableName));
    assertTrue(destMetastore.existsTable(dbName, secondTableName));
  }

  @Test
  public void testDropPartition() throws Exception {
    // Reset the state
    resetState();
    clearMetastores();

    final String dbName = "test_db";
    final String tableName = "test_table";
    final String partitionName = "ds=1/hr=2";
    final HiveObjectSpec partitionSpec = new HiveObjectSpec(dbName, tableName,
        partitionName);

    // Create a partitioned table and a partition on the source, and
    // replicate it.
    simulateCreatePartitionedTable(dbName, tableName);
    simulateCreatePartition(dbName, tableName, partitionName);
    ReplicationServer replicationServer = createReplicationServer();
    replicationServer.run(2);

    // Verify that the partition is on the destination
    assertTrue(destMetastore.existsPartition(dbName, tableName,
          partitionName));

    // Simulate the drop
    LOG.debug("Dropping " + partitionSpec);
    simulateDropPartition(dbName, tableName, partitionName);

    // Run replication so that it picks up the drop command
    replicationServer.setStartAfterAuditLogId(2);
    replicationServer.run(1);

    // Verify that the partition is gone from the destination
    assertFalse(destMetastore.existsPartition(dbName, tableName,
          partitionName));
  }


  @Test
  public void testDropTable() throws Exception {
    // Reset the state
    resetState();
    clearMetastores();

    final String dbName = "test_db";
    final String tableName = "test_table";

    // Create a table on the source, and replicate it
    simulatedCreateUnpartitionedTable(dbName, tableName);
    ReplicationServer replicationServer = createReplicationServer();
    replicationServer.run(1);

    // Verify that the partition is on the destination
    assertTrue(destMetastore.existsTable(dbName, tableName));

    // Simulate the drop
    simulateDropTable(dbName, tableName);

    // Run replication so that it picks up the drop command.
    replicationServer.setStartAfterAuditLogId(1);
    replicationServer.run(1);

    // Verify that the partition is gone from the destination
    assertFalse(destMetastore.existsTable(dbName, tableName));
  }


  /**
   * Test to make sure that the drop table command does not get replicated
   * if the table is modified on the destination.
   *
   * @throws Exception if there is an error setting up or running this test
   */
  @Test
  public void testDropTableNoOp() throws Exception {
    // Reset the state
    resetState();
    clearMetastores();

    final String dbName = "test_db";
    final String tableName = "test_table";
    final String secondTableName = "test_table_2";

    // Create a table on the source, and replicate it
    simulatedCreateUnpartitionedTable(dbName, tableName);
    ReplicationServer replicationServer = createReplicationServer();
    replicationServer.run(1);

    // Verify that the partition is on the destination
    assertTrue(destMetastore.existsTable(dbName, tableName));
    // Simulate the drop
    simulateDropTable(dbName, tableName);

    // Update the modified time on the destination table
    final Table table = destMetastore.getTable(dbName, tableName);
    table.getParameters().put(HiveParameterKeys.TLDT,
        Long.toString(System.currentTimeMillis()));
    destMetastore.alterTable(dbName, tableName, table);

    // Create another table on the source so that replication has something
    // to do on the next invocation if it skips the drop command
    simulatedCreateUnpartitionedTable(dbName, secondTableName);

    // Run replication so that it picks up the drop command
    replicationServer.run(1);

    // Verify that the partition is still there on the destination
    assertTrue(destMetastore.existsTable(dbName, tableName));
  }

  /**
   * Test whether the rename table operation is properly propagated.
   *
   * @throws Exception if there is an error setting up or running this test
   */
  @Test
  public void testRenameTable() throws Exception {
    // Reset the state
    resetState();
    clearMetastores();

    final String dbName = "test_db";
    final String tableName = "test_table";
    final String newTableName = "new_test_table";

    // Create a table on the source, and replicate it
    simulatedCreateUnpartitionedTable(dbName, tableName);
    final ReplicationServer replicationServer = createReplicationServer();
    replicationServer.run(1);

    // Verify that the table is on the destination
    assertTrue(destMetastore.existsTable(dbName, tableName));

    // Simulate the rename
    simulatedRenameTable(dbName, tableName, newTableName, false);

    // Propagate the rename
    replicationServer.setStartAfterAuditLogId(1);
    replicationServer.run(1);

    // Verify that the table is renamed on the destination
    assertFalse(destMetastore.existsTable(dbName, tableName));
    assertTrue(destMetastore.existsTable(dbName, newTableName));
  }

  /**
   * Test whether the rename table operation from THRIFT audit log is properly propagated.
   *
   * @throws Exception if there is an error setting up or running this test
   */
  @Test
  public void testRenameTableByThrift() throws Exception {
    // Reset the state
    resetState();
    clearMetastores();

    final String dbName = "test_db";
    final String tableName = "test_table";
    final String newTableName = "new_test_table";

    // Create a table on the source, and replicate it
    simulatedCreateUnpartitionedTable(dbName, tableName);
    final ReplicationServer replicationServer = createReplicationServer();
    replicationServer.run(1);

    // Verify that the table is on the destination
    assertTrue(destMetastore.existsTable(dbName, tableName));

    // Simulate the rename
    simulatedRenameTable(dbName, tableName, newTableName, true);

    // Propagate the rename
    replicationServer.setStartAfterAuditLogId(1);
    replicationServer.run(1);

    // Verify that the table is renamed on the destination
    assertFalse(destMetastore.existsTable(dbName, tableName));
    assertTrue(destMetastore.existsTable(dbName, newTableName));
  }

  /**
   * Test whether the rename partition operation from THRIFT audit log is properly propagated.
   *
   * @throws Exception if there is an error setting up or running this test
   */
  @Test
  public void testRenamePartitionByThrift() throws Exception {
    // Reset the state
    resetState();
    clearMetastores();

    // Create an partitioned table and a corresponding entry in the audit log
    final String dbName = "test_db";
    final String tableName = "test_table";
    final String newPartitionName = "ds=1/hr=2";
    final String oldPartitionName = "ds=1/hr=1";
    final List<String> newPartitionValues = new ArrayList<>();
    newPartitionValues.add("1"); // for `ds` partition
    newPartitionValues.add("2"); // for `hr` partition

    simulateCreatePartitionedTable(dbName, tableName);
    simulateCreatePartition(dbName, tableName, oldPartitionName);

    // Have the replication server rename it.
    ReplicationServer replicationServer = createReplicationServer();
    replicationServer.run(2);

    // Simulate the rename
    simulatedRenamePartition(dbName,
        tableName,
        oldPartitionName,
        newPartitionValues);
    replicationServer.setStartAfterAuditLogId(2);
    replicationServer.run(1);

    // Verify that the object was renamed
    assertFalse(destMetastore.existsPartition(dbName, tableName, oldPartitionName));
    assertTrue(destMetastore.existsPartition(dbName, tableName, newPartitionName));
  }

  /**
   * Test whether the rename table operation is properly propagated in case
   * when the table is updated on the destination. In such a case, the
   * table should be copied over.
   *
   * @throws Exception if there is an error setting up or running this test
   */
  @Test
  public void testRenameTableCopy() throws Exception {
    // Reset the state
    resetState();
    clearMetastores();

    final String dbName = "test_db";
    final String tableName = "test_table";
    final String newTableName = "new_test_table";

    // Create a table on the source, and replicate it
    simulatedCreateUnpartitionedTable(dbName, tableName);
    final ReplicationServer replicationServer = createReplicationServer();
    replicationServer.run(1);

    // Verify that the table is on the destination
    assertTrue(destMetastore.existsTable(dbName, tableName));

    // Update the modified time on the destination table
    final Table table = destMetastore.getTable(dbName, tableName);
    table.getParameters().put(HiveParameterKeys.TLDT,
        Long.toString(System.currentTimeMillis()));
    destMetastore.alterTable(dbName, tableName, table);

    // Simulate the rename
    simulatedRenameTable(dbName, tableName, newTableName, false);

    // Propagate the rename
    replicationServer.setStartAfterAuditLogId(1);
    replicationServer.run(1);

    // Verify that the renamed table was copied over, and the modified table
    // remains.
    assertTrue(destMetastore.existsTable(dbName, tableName));
    assertTrue(destMetastore.existsTable(dbName, newTableName));
  }

  @Test
  public void testExchangePartition() throws Exception {
    // Reset the state
    resetState();
    clearMetastores();

    // Create an partitioned table in the source and a corresponding
    // entry in the audit log
    String dbName = "test_db";
    String exchangeFromTableName = "test_table_exchange_from";
    String exchangeToTableName = "test_table_exchange_to";
    String partitionName = "ds=1/hr=2";

    simulateCreatePartitionedTable(dbName, exchangeFromTableName);
    simulateCreatePartition(dbName, exchangeFromTableName, partitionName);
    simulateCreatePartitionedTable(dbName, exchangeToTableName);

    // Have the replication server copy it.
    ReplicationServer replicationServer = createReplicationServer();
    replicationServer.run(3);

    // Simulate the exchange
    simulateExchangePartition(dbName,
        exchangeFromTableName,
        dbName,
        exchangeToTableName,
        partitionName);
    replicationServer.setStartAfterAuditLogId(3);
    replicationServer.run(1);

    // Verify that the object was copied
    assertTrue(destMetastore.existsTable(dbName, exchangeFromTableName));
    assertTrue(destMetastore.existsTable(dbName, exchangeToTableName));
    assertTrue(destMetastore.existsPartition(
          dbName,
          exchangeToTableName,
          partitionName));
  }

  @AfterClass
  public static void tearDownClass() {
    MockClusterTest.tearDownClass();
    embeddedMySqlDb.stopDb();
  }

  // Additional cases to test
  // * Copy partition after a restart
  // * Copy unpartitioned table
  // * Different rename cases with filters
  // * Copying partitions after a table schema change
}
