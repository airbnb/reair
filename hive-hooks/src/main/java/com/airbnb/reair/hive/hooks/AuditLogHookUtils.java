package com.airbnb.reair.hive.hooks;

import com.airbnb.reair.db.DbConnectionFactory;
import com.airbnb.reair.db.EmbeddedMySqlDb;
import com.airbnb.reair.db.TestDbCredentials;
import com.airbnb.reair.utils.ReplicationTestUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.ql.MapRedStats;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class AuditLogHookUtils {

  private static final Log LOG = LogFactory.getLog(AuditLogHookUtils.class);

  /**
   * In the MySQL DB, setup the DB and the tables for the audit log to work
   * properly.
   * @param connectionFactory a factory for creating connections to the DB that should contain the
   *                          tables
   * @param dbName the name of the MySQL DB
   * @param auditCoreLogTableName the name of the table containing core audit log data (e.g. query
   *                              string)
   * @param objectsTableName the name of the table containing the serialized Thrift objects
   *                         associated with the query.
   * @param mapRedStatsTableName the name of the table containing the stats about the map-reduce
   *                             jobs associated with the query
   *
   * @throws SQLException if there's an error creating the tables on the DB
   */
  public static void setupAuditLogTables(
      DbConnectionFactory connectionFactory,
      String dbName,
      String auditCoreLogTableName,
      String objectsTableName,
      String mapRedStatsTableName) throws SQLException {

    // Define the SQL that will do the creation
    String createDbSql = String.format("CREATE DATABASE %s", dbName);

    String createAuditLogTableSql =  String.format(
        "CREATE TABLE `%s` ("
            + "`id` bigint(20) NOT NULL AUTO_INCREMENT, "
            + "`create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, "
            + "`query_id` varchar(256) DEFAULT NULL,"
            + "`command_type` varchar(64) DEFAULT NULL,"
            + "`command` mediumtext,"
            + "`inputs` mediumtext,"
            + "`outputs` mediumtext,"
            + "`username` varchar(64) DEFAULT NULL,"
            + "`ip` varchar(64) DEFAULT NULL,"
            + "`extras` mediumtext,"
            + "PRIMARY KEY (`id`),"
            + "KEY `create_time_index` (`create_time`)"
            + ") ENGINE=InnoDB", auditCoreLogTableName);

    String createObjectsTableSql = String.format(
        "CREATE TABLE `%s` ("
            + "`id` bigint(20) NOT NULL AUTO_INCREMENT, "
            + "`create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, "
            + "`audit_log_id` bigint(20) NOT NULL, "
            + "`category` varchar(64) DEFAULT NULL, "
            + "`type` varchar(64) DEFAULT NULL, "
            + "`name` varchar(4000) DEFAULT NULL, "
            + "`serialized_object` mediumtext, "
            + "PRIMARY KEY (`id`), "
            + "KEY `create_time_index` (`create_time`) "
            + ") ENGINE=InnoDB", objectsTableName);

    String createMapRedStatsTableSql = String.format(
        "CREATE TABLE `%s` ("
            + "`id` bigint(20) NOT NULL AUTO_INCREMENT, "
            + "`create_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP, "
            + "`audit_log_id` bigint(20) NOT NULL, "
            + "`stage` varchar(256) NOT NULL, "
            + "`mappers` bigint(20) NOT NULL, "
            + "`reducers` bigint(20) NOT NULL, "
            + "`cpu_time` bigint(20) NOT NULL, "
            + "`counters` text(240000) DEFAULT NULL, "
            + "PRIMARY KEY (`id`), "
            + "KEY `create_time_index` (`create_time`) ,"
            + "KEY `audit_log_id_index` (`audit_log_id`) "
            + ") ENGINE=InnoDB", mapRedStatsTableName);

    // Create the database
    Connection connection = connectionFactory.getConnection();

    Statement statement = connection.createStatement();

    // Create the tables
    try {
      statement.execute(createDbSql);

      connection.setCatalog(dbName);

      statement = connection.createStatement();
      statement.execute(createAuditLogTableSql);
      statement.execute(createObjectsTableSql);
      statement.execute(createMapRedStatsTableSql);
    } finally {
      statement.close();
      connection.close();
    }
  }

  /**
   * Insert an audit log entry that represent a query with the supplied values.
   *
   * @param cliAuditLogHook the CLI audit log hook to use
   * @param operation the type of Hive operation (e.g. ALTER TABLE, QUERY, etc)
   * @param command the command / query string that was run
   * @param inputTables the tables that were read by the query
   * @param inputPartitions the partitions that were read by the query
   * @param outputTables the tables that were modified by the query
   * @param outputPartitions the partitions that were modified by the query
   * @param mapRedStatsPerStage map between the name of the stage and map-reduce job statistics
   * @param hiveConf Hive configuration
   *
   * @throws Exception if there's an error inserting into the audit log
   */
  public static void insertAuditLogEntry(
      CliAuditLogHook cliAuditLogHook,
      HiveOperation operation,
      String command,
      List<Table> inputTables,
      List<org.apache.hadoop.hive.ql.metadata.Partition> inputPartitions,
      List<Table> outputTables,
      List<org.apache.hadoop.hive.ql.metadata.Partition> outputPartitions,
      Map<String, MapRedStats> mapRedStatsPerStage,
      HiveConf hiveConf) throws Exception {

    Set<ReadEntity> readEntities = new HashSet<>();
    Set<WriteEntity> writeEntities = new HashSet<>();

    for (Table t : inputTables) {
      readEntities.add(new ReadEntity(t));
    }

    for (org.apache.hadoop.hive.ql.metadata.Partition p : inputPartitions) {
      readEntities.add(new ReadEntity(p));
    }

    for (Table t : outputTables) {
      writeEntities.add(new WriteEntity(t, WriteEntity.WriteType.DDL_NO_LOCK));
    }

    for (org.apache.hadoop.hive.ql.metadata.Partition p : outputPartitions) {
      writeEntities.add(new WriteEntity(p, WriteEntity.WriteType.DDL_NO_LOCK));
    }

    SessionState sessionState = new SessionState(hiveConf);

    // Map the HiveOperation to the ...ql.plan.HiveOperation when possible.
    // ALTERTABLE_EXCHANGEPARTITION may be the only one that can't be mapped.
    org.apache.hadoop.hive.ql.plan.HiveOperation commandType = null;

    if (operation != null) {
      for (org.apache.hadoop.hive.ql.plan.HiveOperation op :
          org.apache.hadoop.hive.ql.plan.HiveOperation.values()) {
        if (op.toString().equals(operation.toString())) {
          commandType = op;
        }
      }
      if (commandType == null) {
        LOG.warn(String.format("Could not find corresponding enum for %s in %s",
            operation.toString(),
            org.apache.hadoop.hive.ql.plan.HiveOperation.class.getName()));
      }
    }

    sessionState.setMapRedStats(mapRedStatsPerStage);
    SessionState.setCurrentSessionState(sessionState);

    // Run the hook
    SemanticAnalyzer semanticAnalyzer = new SemanticAnalyzer(hiveConf);
    QueryPlan queryPlan = new QueryPlan(
            command,
            semanticAnalyzer,
            null,
            commandType != null ? commandType.getOperationName() : null
    );

    HookContext hookContext = new HookContext(queryPlan, null);
    hookContext.setInputs(readEntities);
    hookContext.setOutputs(writeEntities);
    hookContext.setConf(hiveConf);

    cliAuditLogHook.run(hookContext);
  }

  /**
   * Insert a thrift audit log entry that represents renaming a table.
   *
   * @param oldTable the source table
   * @param newTable the table renamed to
   * @param hiveConf Hive configuration
   * @throws Exception if there's an error inserting into the audit log
   */
  public static void insertThriftRenameTableLogEntry(
      org.apache.hadoop.hive.metastore.api.Table oldTable,
      org.apache.hadoop.hive.metastore.api.Table newTable,
      HiveConf hiveConf) throws Exception {
    final MetastoreAuditLogListener metastoreAuditLogListener =
        new MetastoreAuditLogListener(hiveConf);

    AlterTableEvent event = new AlterTableEvent(
        oldTable,
        newTable,
        true,
        null
    );

    metastoreAuditLogListener.onAlterTable(event);
  }

  /**
   * Insert a thrift audit log entry that represents renaming a partition.
   *
   * @param hmsHandler the HMSHandler for the event
   * @param oldPartition the old partition name
   * @param newPartition the new partition name
   * @param hiveConf Hive configuration
   * @throws Exception if there's an error inserting into the audit log
   */
  public static void insertThriftRenamePartitionLogEntry(
      HiveMetaStore.HMSHandler hmsHandler,
      Partition oldPartition,
      Partition newPartition,
      HiveConf hiveConf) throws Exception {
    final MetastoreAuditLogListener metastoreAuditLogListener =
        new MetastoreAuditLogListener(hiveConf);

    AlterPartitionEvent event = new AlterPartitionEvent(
        oldPartition,
        newPartition,
        true,
        hmsHandler
    );

    metastoreAuditLogListener.onAlterPartition(event);
  }

  /**
   * Get a hive conf filled with config values.
   *
   * @param mySqlDb the database hive should use to write the audit log to
   * @param dbName the name of the database to be used
   * @param auditCoreLogTableName the table name for the core audit log
   * @param outputObjectsTableName the table name for the output objects
   * @param mapRedStatsTableName the table name for the map-reduce stats
   * @return the hive configuration with the config values set
   */
  public static HiveConf getHiveConf(
      EmbeddedMySqlDb mySqlDb,
      String dbName,
      String auditCoreLogTableName,
      String outputObjectsTableName,
      String mapRedStatsTableName) {
    HiveConf hiveConf = new HiveConf();
    hiveConf.set(CliAuditLogHook.JDBC_URL_KEY,
        ReplicationTestUtils.getJdbcUrl(mySqlDb, dbName));
    hiveConf.set(AuditCoreLogModule.TABLE_NAME_KEY, auditCoreLogTableName);
    hiveConf.set(ObjectLogModule.TABLE_NAME_KEY, outputObjectsTableName);
    hiveConf.set(MapRedStatsLogModule.TABLE_NAME_KEY, mapRedStatsTableName);
    return hiveConf;
  }

  /**
   * Get a hive conf for the metastore.
   *
   * @param mySqlDb the database hive should use to write the audit log to
   * @param dbName the name of the database to be used
   * @param auditCoreLogTableName the table name for the core audit log
   * @param outputObjectsTableName the table name for the output objects
   * @return the hive configuration with the config values set
   */
  public static HiveConf getMetastoreHiveConf(
      EmbeddedMySqlDb mySqlDb,
      String dbName,
      String auditCoreLogTableName,
      String outputObjectsTableName) {
    final TestDbCredentials testDbCredentials = new TestDbCredentials();

    HiveConf hiveConf = new HiveConf();

    hiveConf.set(
        MetastoreAuditLogListener.JDBC_URL_KEY,
        ReplicationTestUtils.getJdbcUrl(mySqlDb, dbName)
    );

    hiveConf.set(AuditCoreLogModule.TABLE_NAME_KEY, auditCoreLogTableName);
    hiveConf.set(ObjectLogModule.TABLE_NAME_KEY, outputObjectsTableName);

    hiveConf.set(
        MetastoreAuditLogListener.DB_USERNAME,
        testDbCredentials.getReadWriteUsername()
    );

    hiveConf.set(
        MetastoreAuditLogListener.DB_PASSWORD,
        testDbCredentials.getReadWritePassword()
    );

    return hiveConf;
  }
}
