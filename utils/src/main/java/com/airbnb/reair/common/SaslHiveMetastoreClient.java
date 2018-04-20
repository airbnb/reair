package com.airbnb.reair.common;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import org.apache.hadoop.hive.metastore.IMetaStoreClient;

import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Table;

import org.apache.thrift.TException;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Concrete implementation of secured HiveMetastoreClient.
 */
public class SaslHiveMetastoreClient implements HiveMetastoreClient {

  private IMetaStoreClient client;
  private boolean isConnected;

  public SaslHiveMetastoreClient(IMetaStoreClient client) {
    this.client = client;
    this.isConnected = true;
  }

  @Override
  public synchronized Partition addPartition(Partition partition) throws HiveMetastoreException {
    try {
      connectIfNeeded();
      return client.add_partition(partition);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public synchronized Table getTable(String dbName, String tableName)
      throws HiveMetastoreException {

    try {
      connectIfNeeded();
      Table table = client.getTable(dbName, tableName);
      return table;
    } catch (NoSuchObjectException e) {
      return null;
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public synchronized Partition getPartition(String dbName, String tableName, String partitionName)
      throws HiveMetastoreException {

    try {
      connectIfNeeded();
      return client.getPartition(dbName, tableName, partitionName);
    } catch (NoSuchObjectException e) {
      return null;
    } catch (MetaException e) {
      // Brittle code - this was added to handle an issue with the Hive
      // Metstore. The MetaException is thrown when a table is
      // partitioned with one schema but the name follows a different one.
      // It's impossible to differentiate from that case and other
      // causes of the MetaException without something like this.
      if ("Invalid partition key & values".equals(e.getMessage())) {
        return null;
      } else {
        throw new HiveMetastoreException(e);
      }
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public synchronized void alterPartition(String dbName, String tableName, Partition partition)
      throws HiveMetastoreException {
    try {
      connectIfNeeded();
      client.alter_partition(dbName, tableName, partition);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public synchronized void alterTable(String dbName, String tableName, Table table)
      throws HiveMetastoreException {
    try {
      connectIfNeeded();
      client.alter_table(dbName, tableName, table);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public boolean isPartitioned(String dbName, String tableName) throws HiveMetastoreException {
    Table table = getTable(dbName, tableName);
    return table != null && table.getPartitionKeys().size() > 0;
  }

  @Override
  public synchronized boolean existsPartition(String dbName,
                                              String tableName,
                                              String partitionName)
      throws HiveMetastoreException {
    return getPartition(dbName, tableName, partitionName) != null;
  }

  @Override
  public synchronized boolean existsTable(String dbName, String tableName)
      throws HiveMetastoreException {
    return getTable(dbName, tableName) != null;
  }

  @Override
  public void createTable(Table table) throws HiveMetastoreException {
    try {
      connectIfNeeded();
      client.createTable(table);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }


  @Override
  public synchronized void dropTable(String dbName, String tableName, boolean deleteData)
      throws HiveMetastoreException {
    try {
      connectIfNeeded();
      client.dropTable(dbName, tableName, deleteData, true);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public synchronized void dropPartition(String dbName, String tableName, String partitionName,
      boolean deleteData) throws HiveMetastoreException {
    try {
      connectIfNeeded();
      client.dropPartition(dbName, tableName, partitionName, deleteData);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public synchronized Map<String, String> partitionNameToMap(String partitionName)
      throws HiveMetastoreException {
    try {
      connectIfNeeded();
      return client.partitionNameToSpec(partitionName);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public synchronized void createDatabase(Database db) throws HiveMetastoreException {
    try {
      connectIfNeeded();
      client.createDatabase(db);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public synchronized Database getDatabase(String dbName) throws HiveMetastoreException {
    try {
      connectIfNeeded();
      return client.getDatabase(dbName);
    } catch (NoSuchObjectException e) {
      return null;
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public synchronized boolean existsDb(String dbName) throws HiveMetastoreException {
    return getDatabase(dbName) != null;
  }

  @Override
  public synchronized List<String> getPartitionNames(String dbName, String tableName)
      throws HiveMetastoreException {
    try {
      connectIfNeeded();
      return client.listPartitionNames(dbName, tableName, (short) -1);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public synchronized List<String> getTables(String dbName, String tableName)
      throws HiveMetastoreException {
    try {
      connectIfNeeded();
      return client.getTables(dbName, tableName);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public synchronized Partition exchangePartition(
      Map<String, String> partitionSpecs,
      String sourceDb, String sourceTable,
      String destDb,
      String destinationTableName)
          throws HiveMetastoreException {
    try {
      connectIfNeeded();
      return client.exchange_partition(partitionSpecs, sourceDb, sourceTable, destDb,
          destinationTableName);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public void renamePartition(
      String db,
      String table,
      List<String> partitionValues,
      Partition partition)
      throws HiveMetastoreException {
    try {
      connectIfNeeded();
      client.renamePartition(db, table, partitionValues, partition);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public List<String> getAllDatabases() throws HiveMetastoreException {
    try {
      connectIfNeeded();
      return client.getAllDatabases();
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public List<String> getAllTables(String dbName) throws HiveMetastoreException {
    try {
      connectIfNeeded();
      return client.getAllTables(dbName);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public PrincipalPrivilegeSet listTablePrivileges(String dbName, String tableName)
          throws HiveMetastoreException {
    try {
      connectIfNeeded();
      Set<HiveObjectPrivilege> allPrivs = Sets.newHashSet();
      allPrivs.addAll(this.listHiveTablePrivileges(dbName, tableName, PrincipalType.USER));
      allPrivs.addAll(this.listHiveTablePrivileges(dbName, tableName, PrincipalType.GROUP));
      allPrivs.addAll(this.listHiveTablePrivileges(dbName, tableName, PrincipalType.ROLE));

      List<HiveObjectPrivilege> userPrivs = selectPrivilegesOfType(allPrivs, PrincipalType.USER);
      List<HiveObjectPrivilege> groupPrivs = selectPrivilegesOfType(allPrivs, PrincipalType.GROUP);
      List<HiveObjectPrivilege> rolePrivs = selectPrivilegesOfType(allPrivs, PrincipalType.ROLE);

      PrincipalPrivilegeSet principalPrivs = new PrincipalPrivilegeSet();
      principalPrivs.setUserPrivileges(createGrants(userPrivs));
      principalPrivs.setGroupPrivileges(createGrants(groupPrivs));
      principalPrivs.setRolePrivileges(createGrants(rolePrivs));

      return  principalPrivs;
    } catch (TException ex) {
      close();
      throw new HiveMetastoreException(ex);
    }
  }

  @Override
  public List<HiveObjectPrivilege> listDatabasePrivileges(String dbName)
          throws HiveMetastoreException {
    Set<HiveObjectPrivilege> privileges = Sets.newHashSet();
    try {
      connectIfNeeded();
      privileges.addAll(this.listHiveDatabasePrivileges(dbName, PrincipalType.USER));
      privileges.addAll(this.listHiveDatabasePrivileges(dbName, PrincipalType.GROUP));
      privileges.addAll(this.listHiveDatabasePrivileges(dbName, PrincipalType.ROLE));
      return Lists.newArrayList(privileges.iterator());
    } catch (TException ex) {
      close();
      throw new HiveMetastoreException(ex);
    }
  }

  @Override
  public void grantPrivileges(List<HiveObjectPrivilege> privileges) throws HiveMetastoreException {
    try {
      connectIfNeeded();
      client.grant_privileges(new PrivilegeBag(privileges));
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public void close() {
    this.client.close();
    this.isConnected = false;
  }

  private void connect() throws HiveMetastoreException {
    try {
      this.client.reconnect();
      this.isConnected = true;
    } catch (MetaException e) {
      throw new HiveMetastoreException(e);
    }
  }

  private void connectIfNeeded() throws HiveMetastoreException {
    if (!this.isConnected) {
      this.connect();
      this.isConnected = true;
    }
  }

  private List<HiveObjectPrivilege> selectPrivilegesOfType(
          Set<HiveObjectPrivilege> privileges, PrincipalType type) {
    return privileges.stream()
            .filter(priv -> priv.getPrincipalType() == type)
            .collect(Collectors.toList());
  }

  private Map<String, List<PrivilegeGrantInfo>> createGrants(
          List<HiveObjectPrivilege> hiveObjectPrivileges) throws TException {
    Map<String, List<PrivilegeGrantInfo>> privGrants = Maps.newHashMap();

    Map<String, List<HiveObjectPrivilege>> privilegeGroups = hiveObjectPrivileges.stream()
            .collect(Collectors.groupingBy(HiveObjectPrivilege::getPrincipalName));
    for (String principalName : privilegeGroups.keySet()) {
      List<PrivilegeGrantInfo> grantInfos = privilegeGroups.get(principalName).stream()
              .map(HiveObjectPrivilege::getGrantInfo)
              .collect(Collectors.toList());
      privGrants.put(principalName, grantInfos);
    }

    return privGrants;
  }

  private List<HiveObjectPrivilege> listHiveTablePrivileges(
          String dbName,
          String tableName,
          PrincipalType principalType) throws TException {
    HiveObjectRef hiveObj = new HiveObjectRef(HiveObjectType.TABLE, dbName, tableName, null, null);
    return client.list_privileges(null, principalType, hiveObj);
  }

  private List<HiveObjectPrivilege> listHiveDatabasePrivileges(
          String dbName,
          PrincipalType principalType) throws TException {
    HiveObjectRef hiveObj = new HiveObjectRef(HiveObjectType.DATABASE, dbName, null, null, null);
    return client.list_privileges(null, principalType, hiveObj);
  }
}
