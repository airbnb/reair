package com.airbnb.reair.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.util.List;
import java.util.Map;

/**
 * Concrete implementation of a HiveMetastoreClient using Thrift RPC's.
 */
public class ThriftHiveMetastoreClient implements HiveMetastoreClient {

  private static final Log LOG = LogFactory.getLog(ThriftHiveMetastoreClient.class);

  private static int DEFAULT_SOCKET_TIMEOUT = 600;

  private String host;
  private int port;
  private int clientSocketTimeout;

  private TTransport transport;
  private ThriftHiveMetastore.Client client;

  /**
   * TODO.
   *
   * @param host TODO
   * @param port TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public ThriftHiveMetastoreClient(String host, int port) throws HiveMetastoreException {
    this.host = host;
    this.port = port;
    this.clientSocketTimeout = DEFAULT_SOCKET_TIMEOUT;

    connect();
  }

  /**
   * TODO.
   *
   * @throws HiveMetastoreException TODO
   */
  private void connect() throws HiveMetastoreException {

    LOG.info("Connecting to ThriftHiveMetastore " + host + ":" + port);
    transport = new TSocket(host, port, 1000 * clientSocketTimeout);

    this.client = new ThriftHiveMetastore.Client(new TBinaryProtocol(transport));

    try {
      transport.open();
    } catch (TTransportException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  /**
   * TODO.
   */
  public void close() {
    if (transport != null) {
      transport.close();
      transport = null;
      client = null;
    }
  }

  private void connectIfNeeded() throws HiveMetastoreException {
    if (transport == null) {
      connect();
    }
  }

  /**
   * TODO.
   *
   * @param partition TODO
   * @return TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public synchronized Partition addPartition(Partition partition) throws HiveMetastoreException {
    try {
      connectIfNeeded();
      return client.add_partition(partition);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  /**
   * TODO.
   *
   * @param dbName TODO
   * @param tableName TODO
   * @return TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public synchronized Table getTable(String dbName, String tableName)
      throws HiveMetastoreException {

    try {
      connectIfNeeded();
      return client.get_table(dbName, tableName);
    } catch (NoSuchObjectException e) {
      return null;
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  /**
   * TODO.
   *
   * @param dbName TODO
   * @param tableName TODO
   * @param partitionName TODO
   * @return TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public synchronized Partition getPartition(String dbName, String tableName, String partitionName)
      throws HiveMetastoreException {

    try {
      connectIfNeeded();
      return client.get_partition_by_name(dbName, tableName, partitionName);
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

  /**
   * TODO.
   *
   * @param dbName TODO
   * @param tableName TODO
   * @param partition TODO
   *
   * @throws HiveMetastoreException TODO
   */
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

  /**
   * TODO.
   *
   * @param dbName TODO
   * @param tableName TODO
   * @param table TODO
   *
   * @throws HiveMetastoreException TODO
   */
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

  /**
   * TODO.
   *
   * @param dbName TODO
   * @param tableName TODO
   * @return TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public boolean isPartitioned(String dbName, String tableName) throws HiveMetastoreException {
    Table table = getTable(dbName, tableName);
    return table != null && table.getPartitionKeys().size() > 0;
  }

  /**
   * TODO.
   *
   * @param dbName TODO
   * @param tableName TODO
   * @param partitionName TODO
   * @return TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public synchronized boolean existsPartition(String dbName, String tableName, String partitionName)
      throws HiveMetastoreException {
    return getPartition(dbName, tableName, partitionName) != null;
  }

  /**
   * TODO.
   *
   * @param dbName TODO
   * @param tableName TODO
   * @return TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public synchronized boolean existsTable(String dbName, String tableName)
      throws HiveMetastoreException {
    return getTable(dbName, tableName) != null;
  }

  /**
   * TODO.
   *
   * @param table TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public synchronized void createTable(Table table) throws HiveMetastoreException {
    try {
      connectIfNeeded();
      client.create_table(table);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  /**
   * TODO.
   *
   * @param dbName TODO
   * @param tableName TODO
   * @param deleteData TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public synchronized void dropTable(String dbName, String tableName, boolean deleteData)
      throws HiveMetastoreException {
    try {
      connectIfNeeded();
      client.drop_table(dbName, tableName, deleteData);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  /**
   * TODO.
   *
   * @param dbName TODO
   * @param tableName TODO
   * @param partitionName TODO
   * @param deleteData TODO
   *
   * @throws HiveMetastoreException TODO
   */
  public synchronized void dropPartition(String dbName, String tableName, String partitionName,
      boolean deleteData) throws HiveMetastoreException {
    try {
      connectIfNeeded();
      client.drop_partition_by_name(dbName, tableName, partitionName, deleteData);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  /**
   * TODO.
   *
   * @param partitionName TODO
   * @return TODO
   */
  public synchronized Map<String, String> partitionNameToMap(String partitionName)
      throws HiveMetastoreException {
    try {
      connectIfNeeded();
      return client.partition_name_to_spec(partitionName);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public synchronized void createDatabase(Database db) throws HiveMetastoreException {
    try {
      connectIfNeeded();
      client.create_database(db);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  @Override
  public synchronized Database getDatabase(String dbName) throws HiveMetastoreException {
    try {
      connectIfNeeded();
      return client.get_database(dbName);
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
      return client.get_partition_names(dbName, tableName, (short) -1);
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
      return client.get_tables(dbName, tableName);
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
      client.rename_partition(db, table, partitionValues, partition);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  /**
   * TODO.
   *
   * @return TODO
   */
  public List<String> getAllDatabases() throws HiveMetastoreException {
    try {
      connectIfNeeded();
      return client.get_all_databases();
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }

  /**
   * TODO.
   *
   * @param dbName TODO
   * @return TODO
   */
  public List<String> getAllTables(String dbName) throws HiveMetastoreException {
    try {
      connectIfNeeded();
      return client.get_all_tables(dbName);
    } catch (TException e) {
      close();
      throw new HiveMetastoreException(e);
    }
  }
}
