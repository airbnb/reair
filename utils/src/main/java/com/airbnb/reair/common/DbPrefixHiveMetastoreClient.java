package com.airbnb.reair.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A HiveMetastoreClient wrapper that automatically adds a prefix to DbName when talking with the
 * underlying HiveMetastoreClient.  This is mainly for test purposes.
 */
public class DbPrefixHiveMetastoreClient implements HiveMetastoreClient {

  private static final Log LOG = LogFactory.getLog(DbPrefixHiveMetastoreClient.class);

  private HiveMetastoreClient client;
  private String prefix;

  public DbPrefixHiveMetastoreClient(HiveMetastoreClient client, String prefix) {
    this.client = client;
    this.prefix = prefix;
  }

  private String addPrefix(String dbName) {
    return prefix + dbName;
  }

  private Database addPrefix(Database db) {
    if (db == null) {
      return null;
    }
    Database result = new Database(db);
    result.setName(addPrefix(result.getName()));
    return result;
  }

  private Table addPrefix(Table table) {
    if (table == null) {
      return null;
    }
    Table result = new Table(table);
    result.setDbName(addPrefix(result.getDbName()));
    return result;
  }

  private Partition addPrefix(Partition partition) {
    if (partition == null) {
      return null;
    }
    Partition result = new Partition(partition);
    result.setDbName(addPrefix(result.getDbName()));
    return result;
  }

  private String removePrefix(String dbName) {
    if (!dbName.startsWith(prefix)) {
      LOG.warn("Cannot remove prefix " + prefix + " from String " + dbName);
    } else {
      dbName = dbName.substring(prefix.length());
    }
    return dbName;
  }

  private Database removePrefix(Database db) {
    if (db == null) {
      return null;
    }
    Database result = new Database(db);
    String dbName = result.getName();
    if (!dbName.startsWith(prefix)) {
      LOG.warn("Cannot remove prefix " + prefix + " from Database " + dbName);
    } else {
      dbName = dbName.substring(prefix.length());
    }
    result.setName(dbName);
    return result;
  }

  private Table removePrefix(Table table) {
    if (table == null) {
      return null;
    }
    Table result = new Table(table);
    String dbName = result.getDbName();
    if (!dbName.startsWith(prefix)) {
      LOG.warn("Cannot remove prefix " + prefix + " from Table " + dbName + ":" + result
          .getTableName());
    } else {
      dbName = dbName.substring(prefix.length());
    }
    result.setDbName(dbName);
    return result;
  }

  private Partition removePrefix(Partition partition) {
    if (partition == null) {
      return null;
    }
    Partition result = new Partition(partition);
    String dbName = result.getDbName();
    if (!dbName.startsWith(prefix)) {
      LOG.warn("Cannot remove prefix " + prefix + " from Partition " + dbName + ":" + result
          .getTableName() + ":" + result.getParameters());
    } else {
      dbName = dbName.substring(prefix.length());
    }
    result.setDbName(dbName);
    return result;
  }

  @Override
  public Partition addPartition(Partition partition) throws HiveMetastoreException {
    return removePrefix(client.addPartition(addPrefix(partition)));
  }

  @Override
  public Table getTable(String dbName, String tableName) throws HiveMetastoreException {
    return removePrefix(client.getTable(addPrefix(dbName), tableName));
  }

  @Override
  public Partition getPartition(String dbName, String tableName, String partitionName) throws
      HiveMetastoreException {
    return removePrefix(client.getPartition(addPrefix(dbName), tableName, partitionName));
  }

  @Override
  public List<String> getPartitionNames(String dbName, String tableName) throws
      HiveMetastoreException {
    return client.getPartitionNames(addPrefix(dbName), tableName);
  }

  @Override
  public void alterPartition(String dbName, String tableName, Partition partition) throws
      HiveMetastoreException {
    client.alterPartition(addPrefix(dbName), tableName, addPrefix(partition));
  }

  @Override
  public void alterTable(String dbName, String tableName, Table table) throws
      HiveMetastoreException {
    client.alterTable(addPrefix(dbName), tableName, addPrefix(table));
  }

  @Override
  public boolean isPartitioned(String dbName, String tableName) throws HiveMetastoreException {
    return client.isPartitioned(addPrefix(dbName), tableName);
  }

  @Override
  public boolean existsPartition(String dbName, String tableName, String partitionName) throws
      HiveMetastoreException {
    return client.existsPartition(addPrefix(dbName), tableName, partitionName);
  }

  @Override
  public boolean existsTable(String dbName, String tableName) throws HiveMetastoreException {
    return client.existsTable(addPrefix(dbName), tableName);
  }

  @Override
  public void createTable(Table table) throws HiveMetastoreException {
    client.createTable(addPrefix(table));
  }

  @Override
  public void dropTable(String dbName, String tableName, boolean deleteData) throws
      HiveMetastoreException {
    client.dropTable(addPrefix(dbName), tableName, deleteData);
  }

  @Override
  public void dropPartition(String dbName, String tableName, String partitionName, boolean
      deleteData) throws HiveMetastoreException {
    client.dropPartition(addPrefix(dbName), tableName, partitionName, deleteData);
  }

  @Override
  public Map<String, String> partitionNameToMap(String partitionName) throws
      HiveMetastoreException {
    return client.partitionNameToMap(partitionName);
  }

  @Override
  public void createDatabase(Database db) throws HiveMetastoreException {
    client.createDatabase(addPrefix(db));
  }

  @Override
  public Database getDatabase(String dbName) throws HiveMetastoreException {
    return removePrefix(client.getDatabase(addPrefix(dbName)));
  }

  @Override
  public boolean existsDb(String dbName) throws HiveMetastoreException {
    return client.existsDb(addPrefix(dbName));
  }

  @Override
  public List<String> getTables(String dbName, String tableName) throws HiveMetastoreException {
    return client.getTables(addPrefix(dbName), tableName);
  }

  @Override
  public Partition exchangePartition(Map<String, String> partitionSpecs, String sourceDb, String
      sourceTable, String destDb, String destinationTableName) throws HiveMetastoreException {
    return removePrefix(client.exchangePartition(partitionSpecs, addPrefix(sourceDb),
        sourceTable, addPrefix(destDb), destinationTableName));
  }

  @Override
  public void renamePartition(String db, String table, List<String> partitionValues, Partition
      partition) throws HiveMetastoreException {
    client.renamePartition(addPrefix(db), table, partitionValues, addPrefix(partition));
  }

  /**
   * Return all databases that start with the predefined prefix.
   */
  @Override
  public List<String> getAllDatabases() throws HiveMetastoreException {
    List<String> result = client.getAllDatabases();
    if (result == null) {
      return null;
    }
    List<String> realResult = new ArrayList<String>();
    for (int i = 0; i < result.size(); i++) {
      String db = result.get(i);
      // Ignore databases that doesn't start with the prefix.
      if (db.startsWith(prefix)) {
        realResult.add(removePrefix(db));
      }
    }
    return realResult;
  }

  @Override
  public List<String> getAllTables(String dbName) throws HiveMetastoreException {
    return client.getAllTables(addPrefix(dbName));
  }

  @Override
  public void close() {
    client.close();
  }
}
