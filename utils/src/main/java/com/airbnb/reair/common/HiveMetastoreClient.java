package com.airbnb.reair.common;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.List;
import java.util.Map;

/**
 * A client for the Hive Metastore Thrift service.
 */
public interface HiveMetastoreClient {

  Partition addPartition(Partition partition) throws HiveMetastoreException;

  Table getTable(String dbName, String tableName) throws HiveMetastoreException;

  Partition getPartition(String dbName, String tableName, String partitionName)
      throws HiveMetastoreException;

  List<String> getPartitionNames(String dbName, String tableName)
      throws HiveMetastoreException;

  void alterPartition(String dbName, String tableName, Partition partition)
      throws HiveMetastoreException;

  void alterTable(
      String dbName,
      String tableName,
      Table table) throws HiveMetastoreException;

  boolean isPartitioned(String dbName, String tableName) throws HiveMetastoreException;

  boolean existsPartition(String dbName, String tableName, String partitionName)
      throws HiveMetastoreException;

  boolean existsTable(String dbName, String tableName) throws HiveMetastoreException;

  void createTable(Table table) throws HiveMetastoreException;

  void dropTable(String dbName, String tableName, boolean deleteData)
      throws HiveMetastoreException;

  void dropPartition(String dbName, String tableName, String partitionName,
                     boolean deleteData) throws HiveMetastoreException;

  Map<String, String> partitionNameToMap(String partitionName) throws HiveMetastoreException;

  void createDatabase(Database db) throws HiveMetastoreException;

  Database getDatabase(String dbName) throws HiveMetastoreException;

  boolean existsDb(String dbName) throws HiveMetastoreException;

  List<String> getTables(String dbName, String tableName) throws HiveMetastoreException;

  Partition exchangePartition(
      Map<String, String> partitionSpecs,
      String sourceDb,
      String sourceTable,
      String destDb,
      String destinationTableName)
      throws HiveMetastoreException;

  void renamePartition(
      String db,
      String table,
      List<String> partitionValues,
      Partition partition)
      throws HiveMetastoreException;

  List<String> getAllDatabases() throws HiveMetastoreException;

  List<String> getAllTables(String dbName) throws HiveMetastoreException;

  void close();

  PrincipalPrivilegeSet listTablePrivileges(String dbName, String tableName)
          throws HiveMetastoreException;

  List<HiveObjectPrivilege> listDatabasePrivileges(String dbName) throws HiveMetastoreException;

  void grantPrivileges(List<HiveObjectPrivilege> privileges) throws HiveMetastoreException;
}
