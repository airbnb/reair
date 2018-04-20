package test;

import com.google.common.collect.Lists;

import com.airbnb.reair.common.HiveMetastoreClient;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Simulates a Hive metastore client connected to a Hive metastore Thrift server.
 */
public class MockHiveMetastoreClient implements HiveMetastoreClient {

  private Map<String, Database> dbNameToDatabase;
  private Map<HiveObjectSpec, Table> specToTable;
  private Map<HiveObjectSpec, Partition> specToPartition;

  /**
   * Creates a Hive metastore client that simulates the behavior of the Hive metastore.
   */
  public MockHiveMetastoreClient() {
    dbNameToDatabase = new HashMap<>();
    specToTable = new HashMap<>();
    specToPartition = new HashMap<>();
  }

  /**
   * Returns the partition name (e.g. ds=1/hr=2) given a Table and Partition object. For simplicity,
   * this does not handle special characters properly.
   *
   * @param table the table that the partition belongs to
   * @param partition the partition to get the name for
   * @return the name of the partition
   * @throws HiveMetastoreException if the schema between the table and partition do not match
   */
  private String getPartitionName(Table table, Partition partition) throws HiveMetastoreException {
    if (table.getPartitionKeys().size() != partition.getValues().size()) {
      throw new HiveMetastoreException(
          "Partition column mismatch: " + "table has " + table.getPartitionKeys().size()
              + " columns " + "while partition has " + partition.getValues().size() + " values");
    }

    List<String> keyValues = new ArrayList<>();
    int keyValueIndex = 0;
    for (FieldSchema field : table.getPartitionKeys()) {
      keyValues.add(field.getName() + "=" + partition.getValues().get(keyValueIndex));
      keyValueIndex++;
    }
    return StringUtils.join(keyValues, "/");
  }

  private String getPartitionName(Table table, List<String> values) {
    StringBuilder sb = new StringBuilder();
    int index = 0;
    for (FieldSchema fs : table.getPartitionKeys()) {
      if (index > 0) {
        sb.append("/");
      }
      sb.append(fs.getName());
      sb.append("=");
      sb.append(values.get(index));
      index++;
    }
    return sb.toString();
  }


  @Override
  public Partition addPartition(Partition partition) throws HiveMetastoreException {
    HiveObjectSpec tableSpec = new HiveObjectSpec(partition.getDbName(), partition.getTableName());
    if (!specToTable.containsKey(tableSpec)) {
      throw new HiveMetastoreException("Unknown table: " + tableSpec);
    }
    Table table = specToTable.get(tableSpec);
    String partitionName = getPartitionName(table, partition);

    HiveObjectSpec partitionSpec =
        new HiveObjectSpec(tableSpec.getDbName(), tableSpec.getTableName(), partitionName);

    if (specToPartition.containsKey(partitionSpec)) {
      throw new HiveMetastoreException("Partition already exists: " + partitionSpec);
    }

    specToPartition.put(partitionSpec, partition);
    return partition;
  }

  @Override
  public Table getTable(String dbName, String tableName) throws HiveMetastoreException {
    return specToTable.get(new HiveObjectSpec(dbName, tableName));
  }

  @Override
  public Partition getPartition(String dbName, String tableName, String partitionName)
      throws HiveMetastoreException {
    return specToPartition.get(new HiveObjectSpec(dbName, tableName, partitionName));
  }

  @Override
  public void alterPartition(String dbName, String tableName, Partition partition)
      throws HiveMetastoreException {
    HiveObjectSpec tableSpec = new HiveObjectSpec(partition.getDbName(), partition.getTableName());
    if (!specToTable.containsKey(tableSpec)) {
      throw new HiveMetastoreException("Unknown table: " + tableSpec);
    }
    Table table = specToTable.get(tableSpec);
    String partitionName = getPartitionName(table, partition);

    HiveObjectSpec partitionSpec =
        new HiveObjectSpec(tableSpec.getDbName(), tableSpec.getTableName(), partitionName);
    if (!specToPartition.containsKey(partitionSpec)) {
      throw new HiveMetastoreException("Partition does not exist: " + partitionSpec);
    }

    specToPartition.put(partitionSpec, partition);
  }

  @Override
  public void createDatabase(Database db) throws HiveMetastoreException {
    if (dbNameToDatabase.containsKey(db.getName())) {
      throw new HiveMetastoreException("DB " + db.getName() + " already exists!");
    }
    dbNameToDatabase.put(db.getName(), db);
  }

  @Override
  public Database getDatabase(String dbName) throws HiveMetastoreException {
    return dbNameToDatabase.get(dbName);
  }

  @Override
  public boolean existsDb(String dbName) throws HiveMetastoreException {
    return getDatabase(dbName) != null;
  }

  @Override
  public void createTable(Table table) throws HiveMetastoreException {
    if (!existsDb(table.getDbName())) {
      throw new HiveMetastoreException("DB " + table.getDbName() + " does not exist!");
    }

    HiveObjectSpec tableSpec = new HiveObjectSpec(table.getDbName(), table.getTableName());
    if (specToTable.containsKey(tableSpec)) {
      throw new HiveMetastoreException("Table already exists: " + tableSpec);
    }
    specToTable.put(tableSpec, table);
  }

  @Override
  public void alterTable(
      String dbName,
      String tableName,
      Table table) throws HiveMetastoreException {
    HiveObjectSpec existingTableSpec = new HiveObjectSpec(dbName, tableName);
    HiveObjectSpec newTableSpec = new HiveObjectSpec(table.getDbName(), table.getTableName());
    if (!specToTable.containsKey(existingTableSpec)) {
      throw new HiveMetastoreException("Unknown table: " + existingTableSpec);
    }
    Table removedTable = specToTable.remove(existingTableSpec);
    if (removedTable == null) {
      throw new RuntimeException("Shouldn't happen!");
    }
    specToTable.put(newTableSpec, table);
  }

  @Override
  public boolean isPartitioned(String dbName, String tableName) throws HiveMetastoreException {
    return getTable(dbName, tableName).getPartitionKeys().size() > 0;
  }

  @Override
  public boolean existsPartition(String dbName, String tableName, String partitionName)
      throws HiveMetastoreException {
    return getPartition(dbName, tableName, partitionName) != null;
  }

  @Override
  public boolean existsTable(String dbName, String tableName) throws HiveMetastoreException {
    return getTable(dbName, tableName) != null;
  }

  /**
   * Drops the table, but for safety, doesn't delete the data.
   */
  @Override
  public void dropTable(String dbName, String tableName, boolean deleteData)
      throws HiveMetastoreException {
    HiveObjectSpec tableSpec = new HiveObjectSpec(dbName, tableName);
    if (!existsTable(dbName, tableName)) {
      throw new HiveMetastoreException("Missing table: " + tableSpec);
    }
    // Remove the table
    specToTable.remove(new HiveObjectSpec(dbName, tableName));

    // Remove associated partitions
    Iterator<Map.Entry<HiveObjectSpec, Partition>> mapIterator =
        specToPartition.entrySet().iterator();

    while (mapIterator.hasNext()) {
      Map.Entry<HiveObjectSpec, Partition> entry = mapIterator.next();
      if (entry.getKey().getTableSpec().equals(tableSpec)) {
        mapIterator.remove();
      }
    }
    // For safety, don't delete data.
  }

  /**
   * Drops the partition, but for safety, doesn't delete the data.
   */
  @Override
  public void dropPartition(
      String dbName,
      String tableName,
      String partitionName,
      boolean deleteData) throws HiveMetastoreException {
    HiveObjectSpec partitionSpec = new HiveObjectSpec(dbName, tableName, partitionName);
    if (!existsPartition(dbName, tableName, partitionName)) {
      throw new HiveMetastoreException("Missing partition: " + partitionSpec);
    }
    specToPartition.remove(partitionSpec);
  }

  @Override
  public List<String> getPartitionNames(String dbName, String tableName)
      throws HiveMetastoreException {
    List<String> partitionNames = new ArrayList<>();
    HiveObjectSpec tableSpec = new HiveObjectSpec(dbName, tableName);
    for (Map.Entry<HiveObjectSpec, Partition> entry : specToPartition.entrySet()) {
      if (tableSpec.equals(entry.getKey().getTableSpec())) {
        partitionNames.add(entry.getKey().getPartitionName());
      }
    }
    return partitionNames;
  }

  @Override
  public Map<String, String> partitionNameToMap(String partitionName)
      throws HiveMetastoreException {
    LinkedHashMap<String, String> partitionKeyToValue = new LinkedHashMap<>();
    String[] keyValues = partitionName.split("/");
    for (String keyValue : keyValues) {
      String[] keyValueSplit = keyValue.split("=");
      String key = keyValueSplit[0];
      String value = keyValueSplit[1];
      partitionKeyToValue.put(key, value);
    }

    return partitionKeyToValue;
  }

  @Override
  public List<String> getTables(String dbName, String tableName) throws HiveMetastoreException {
    if (!tableName.equals("*")) {
      throw new RuntimeException("Only * (wildcard) is supported in " + "the mock client");
    }
    List<String> tableNames = new ArrayList<>();

    for (HiveObjectSpec spec : specToTable.keySet()) {
      if (spec.getDbName().equals(dbName)) {
        tableNames.add(spec.getTableName());
      }
    }
    return tableNames;
  }

  /**
   * Converts a map of partition key-value pairs to a name. Note that special characters are not
   * escaped unlike in production, and the order of the key is dictated by the iteration order for
   * the map.
   */
  private static String partitionSpecToName(Map<String, String> spec) {
    StringBuilder sb = new StringBuilder();
    for (Map.Entry<String, String> entry : spec.entrySet()) {
      if (sb.length() != 0) {
        sb.append("/");
      }
      sb.append(entry.getKey() + "=" + entry.getValue());
    }
    return sb.toString();
  }

  @Override
  public Partition exchangePartition(
      Map<String, String> partitionSpecs,
      String sourceDb,
      String sourceTable,
      String destDb,
      String destinationTableName)
          throws HiveMetastoreException {
    final String partitionName = partitionSpecToName(partitionSpecs);
    final HiveObjectSpec exchangeFromPartitionSpec =
        new HiveObjectSpec(sourceDb, sourceTable, partitionName);
    final HiveObjectSpec exchangeToPartitionSpec =
        new HiveObjectSpec(destDb, destinationTableName, partitionName);

    if (!existsPartition(sourceDb, sourceTable, partitionName)) {
      throw new HiveMetastoreException(
          String.format("Unknown source partition %s.%s/%s", sourceDb, sourceTable, partitionName));
    }

    if (!existsTable(destDb, destinationTableName)) {
      throw new HiveMetastoreException(
          String.format("Unknown destination table %s.%s", destDb, destinationTableName));
    }

    Partition partition = specToPartition.remove(exchangeFromPartitionSpec);
    partition.setDbName(destDb);
    partition.setTableName(destinationTableName);
    specToPartition.put(exchangeToPartitionSpec, partition);
    return partition;
  }

  @Override
  public void renamePartition(
      String db,
      String tableName,
      List<String> partitionValues,
      Partition partition)
          throws HiveMetastoreException {
    HiveObjectSpec tableSpec = new HiveObjectSpec(db, tableName);
    Table table = specToTable.get(tableSpec);

    String renameFromPartitionName = getPartitionName(table, partitionValues);
    String renameToPartitionName = getPartitionName(table, partition.getValues());

    HiveObjectSpec renameFromSpec = new HiveObjectSpec(db, tableName, renameFromPartitionName);
    HiveObjectSpec renameToSpec = new HiveObjectSpec(db, tableName, renameToPartitionName);

    if (specToPartition.containsKey(renameToSpec)) {
      throw new HiveMetastoreException("Partition already exists: " + renameToSpec);
    }

    if (!specToPartition.containsKey(renameFromSpec)) {
      throw new HiveMetastoreException("Partition doesn't exist: " + renameFromPartitionName);
    }

    Partition removed = specToPartition.remove(renameFromSpec);
    removed.setValues(new ArrayList<>(partition.getValues()));
    specToPartition.put(renameToSpec, removed);
  }

  @Override
  public List<String> getAllDatabases() throws HiveMetastoreException {
    return Lists.newArrayList(dbNameToDatabase.keySet());
  }

  @Override
  public List<String> getAllTables(final String dbName) throws HiveMetastoreException {
    ArrayList<String> tables = new ArrayList<>();

    for (HiveObjectSpec spec : specToTable.keySet()) {
      if (spec.getDbName().equals(dbName)) {
        tables.add(spec.getTableName());
      }
    }

    return tables;
  }

  @Override
  public void close() {

  }

  @Override
  public PrincipalPrivilegeSet listTablePrivileges(String dbName, String tableName)
          throws HiveMetastoreException {
    return new PrincipalPrivilegeSet();
  }

  @Override
  public List<HiveObjectPrivilege> listDatabasePrivileges(String dbName)
          throws HiveMetastoreException {
    return Collections.emptyList();
  }

  @Override
  public void grantPrivileges(List<HiveObjectPrivilege> privileges)
          throws HiveMetastoreException {
  }
}
