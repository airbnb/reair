package com.airbnb.reair.incremental;

import com.google.common.base.Joiner;

import com.airbnb.reair.common.HiveMetastoreClient;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.common.HiveParameterKeys;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TJSONProtocol;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.ObjectWriter;
import org.codehaus.jackson.type.TypeReference;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ReplicationUtils {
  private static final Log LOG = LogFactory.getLog(ReplicationUtils.class);

  // For doing exponential backoff, the number of seconds to use as the base
  private static final int DEFAULT_WAIT_TIME_BASE = 2;
  // For doing exponential backoff, the maximum number of seconds to use
  private static final int DEFAULT_MAX_WAIT_TIME = 3600;

  /**
   * Remove (or set to 0) fields in the table object that should not be compared.
   *
   * @param table the table to remove non-comparable fields
   * @return the table with non-comparable fields removed
   */
  public static Table stripNonComparables(Table table) {
    Table newTable = new Table(table);
    newTable.setCreateTime(0);
    newTable.setLastAccessTime(0);
    return newTable;
  }

  /**
   * Remove (or set to 0) fields in the partition object that should not be compared.
   *
   * @param partition the partition to remove non-comparable fields
   * @return the partition with non-comparable fields removed
   */
  public static Partition stripNonComparables(Partition partition) {
    Partition newPartition = new Partition(partition);
    newPartition.setCreateTime(0);
    newPartition.setLastAccessTime(0);
    return newPartition;
  }

  /**
   * Deserialize a Thrift object from JSON.
   *
   * @param serializedObject the JSON string representation of the object
   * @param obj the Thrift object to populate fields
   *
   * @throws MetadataException if there is an error deserializing
   */
  public static <T extends TBase> void deserializeObject(String serializedObject, T obj)
      throws MetadataException {
    TDeserializer deserializer = new TDeserializer(new TJSONProtocol.Factory());

    try {
      deserializer.deserialize(obj, serializedObject, "UTF-8");
    } catch (TException e) {
      throw new MetadataException(e);
    }
  }

  /**
   * Create the database on the destination if it exists on the source but it does not exist on the
   * destination metastore.
   *
   * @param srcMs source Hive metastore
   * @param destMs destination Hive metastore
   * @param dbName DB to create
   * @param copyGrants whether to copy db grants
   *
   * @throws HiveMetastoreException if there's an error creating the DB.
   */
  public static void createDbIfNecessary(HiveMetastoreClient srcMs, HiveMetastoreClient destMs,
      String dbName, boolean copyGrants) throws HiveMetastoreException {
    if (destMs.existsDb(dbName)) {
      LOG.debug("DB " + dbName + " already exists on destination.");
      return;
    } else {
      Database srcDb = srcMs.getDatabase(dbName);
      if (srcDb == null) {
        LOG.warn(String.format("DB %s doesn't exist on the source!", dbName));
        return;
      }
      Database dbToCreate = new Database(srcDb.getName(), srcDb.getDescription(), null, null);
      if (copyGrants) {
        dbToCreate.setOwnerName(srcDb.getOwnerName());
        dbToCreate.setOwnerType(srcDb.getOwnerType());
      }

      LOG.debug("Creating DB: " + dbToCreate);
      destMs.createDatabase(dbToCreate);

      if (copyGrants) {
        destMs.grantPrivileges(srcMs.listDatabasePrivileges(srcDb.getName()));
      }
    }
  }

  /**
   * Check if the specified Hive object exists.
   *
   * @param ms Hive metastore
   * @param spec specification for the Hive object
   * @return whether or not the object exists
   *
   * @throws HiveMetastoreException if there is an error querying the metastore
   */
  public static boolean exists(HiveMetastoreClient ms, HiveObjectSpec spec)
      throws HiveMetastoreException {
    if (spec.isPartition()) {
      return ms.existsPartition(spec.getDbName(), spec.getTableName(), spec.getPartitionName());
    } else {
      return ms.existsTable(spec.getDbName(), spec.getTableName());
    }
  }

  /**
   * Check if the schema between two tables match.
   *
   * @param srcTable source table
   * @param destTable destination table
   * @return whether or not the schemas of the tables match
   */
  public static boolean schemasMatch(Table srcTable, Table destTable) {
    return srcTable.getSd().getCols().equals(destTable.getSd().getCols())
        && srcTable.getPartitionKeys().equals(destTable.getPartitionKeys());
  }

  /**
   * Check if the table has the expected modified time.
   *
   * @param expectedTldt expected modified time
   * @param table table to check
   * @return whether or not table has the expected modified time
   */
  public static boolean transientLastDdlTimesMatch(String expectedTldt, Table table) {
    return StringUtils.equals(expectedTldt, table.getParameters().get(HiveParameterKeys.TLDT));
  }

  /**
   * Check if the partition has the expected modified time.
   *
   * @param expectedTldt expected modified time.
   * @param partition partition to check
   * @return whether or not partition has the expected modified time
   */
  public static boolean transientLastDdlTimesMatch(String expectedTldt, Partition partition) {
    return StringUtils.equals(expectedTldt, partition.getParameters().get(HiveParameterKeys.TLDT));
  }

  /**
   * Check if two tables have matching modified times.
   *
   * @param table1 reference table to compare
   * @param table2 other table to compare
   * @return whether or not the two tables have matching modified times
   */
  public static boolean transientLastDdlTimesMatch(Table table1, Table table2) {
    if (table1 == null || table2 == null) {
      return false;
    }

    return StringUtils.equals(table1.getParameters().get(HiveParameterKeys.TLDT),
        table2.getParameters().get(HiveParameterKeys.TLDT));
  }

  /**
   * Check if two partitions have matching modified times.
   *
   * @param partition1 reference partition to compare
   * @param partition2 other partition to compare
   * @return whether or not the two partitions have matching modified times
   */
  public static boolean transientLastDdlTimesMatch(Partition partition1, Partition partition2) {
    if (partition1 == null || partition2 == null) {
      return false;
    }

    return StringUtils.equals(partition1.getParameters().get(HiveParameterKeys.TLDT),
        partition2.getParameters().get(HiveParameterKeys.TLDT));
  }

  /**
   * Convert JSON representation of a list into a Java string list.
   *
   * @param json JSON representation of a list
   * @return Java string list
   */
  public static List<String> convertToList(String json) {
    try {
      ObjectMapper om = new ObjectMapper();
      return om.readValue(json, new TypeReference<List<String>>() {});
    } catch (IOException e) {
      return null;
    }
  }

  /**
   * Convert JSON representation of a map into a Java string map.
   *
   * @param json JSON representation of a map
   * @return Java string map
   */
  public static Map<String, String> convertToMap(String json) {
    try {
      ObjectMapper om = new ObjectMapper();
      return om.readValue(json, new TypeReference<Map<String, String>>() {});
    } catch (IOException e) {
      return null;
    }
  }

  /**
   * Convert a Java list to a JSON list.
   *
   * @param list list to convert
   * @return JSON representation of the list
   *
   * @throws IOException if there is an error converting the list
   */
  public static String convertToJson(List<String> list) throws IOException {
    // writerWithDefaultPrettyPrinter() bundled in with CDH is not present,
    // so using this deprecated method.
    @SuppressWarnings("deprecation")
    ObjectWriter ow = new ObjectMapper().defaultPrettyPrintingWriter();
    return ow.writeValueAsString(list);
  }

  /**
   * Convert a Java map to a JSON map.
   *
   * @param map map to convert
   * @return the JSON representation of the map
   *
   * @throws IOException if there's an error converting the map
   */
  public static String convertToJson(Map<String, String> map) throws IOException {
    // writerWithDefaultPrettyPrinter() bundled in with CDH is not present,
    // so using this deprecated method.
    @SuppressWarnings("deprecation")
    ObjectWriter ow = new ObjectMapper().defaultPrettyPrintingWriter();
    return ow.writeValueAsString(map);
  }

  /**
   * Get the data location of a Hive table.
   *
   * @param table Thrift Hive table
   * @return the data location of the given table, if present
   */
  public static Optional<Path> getLocation(Table table) {
    if (table == null || table.getSd() == null || table.getSd().getLocation() == null) {
      return Optional.empty();
    } else {
      return Optional.ofNullable(new Path(table.getSd().getLocation()));
    }
  }

  /**
   * Get the data location of a Hive partition.
   *
   * @param partition Thrift Hive partition
   * @return the data location of the given partition, if present
   */
  public static Optional<Path> getLocation(Partition partition) {
    if (partition == null || partition.getSd() == null || partition.getSd().getLocation() == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(new Path(partition.getSd().getLocation()));
  }

  /**
   * Get the last modified time of a Hive table.
   *
   * @param table Thrift Hive table
   * @return the last modified time as a string
   */
  public static Optional<String> getTldt(Table table) {
    if (table == null || table.getParameters() == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(table.getParameters().get(HiveParameterKeys.TLDT));
  }

  /**
   * Get the last modified time of a Hive partition.
   *
   * @param partition Thrift Hive partition
   * @return the last modified time as a string
   */
  public static Optional<String> getTldt(Partition partition) {
    if (partition == null || partition.getParameters() == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(partition.getParameters().get(HiveParameterKeys.TLDT));
  }

  /**
   * Sleep the current thread.
   *
   * @param sleepTime number of miliseconds to sleep for
   */
  public static void sleep(long sleepTime) {
    try {
      Thread.sleep(sleepTime);
    } catch (InterruptedException e) {
      LOG.error("Unexpectedly interrupted!");
    }
  }

  /**
   * Check to see that all of the partitions are from the same table.
   *
   * @param partitions partitions to check
   * @return whether or not all the partitions are from the same table
   */
  public static boolean fromSameTable(Collection<Partition> partitions) {
    if (partitions.size() == 0) {
      return false;
    }
    String dbName = null;
    String tableName = null;

    for (Partition p : partitions) {
      if (dbName == null) {
        dbName = p.getDbName();
      } else if (!dbName.equals(p.getDbName())) {
        return false;
      }

      if (tableName == null) {
        tableName = p.getTableName();
      } else if (!tableName.equals(p.getTableName())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Return a common parent directory of the given directories.
   *
   * @param dirs directories to check
   * @return the common parent directory
   */
  public static Optional<Path> getCommonDirectory(Set<Path> dirs) {
    if (dirs.size() == 0) {
      return Optional.empty();
    }
    // First verify that all the schemes and authorities are the same
    String scheme = null;
    String authority = null;

    for (Path dir : dirs) {
      if (scheme == null) {
        scheme = dir.toUri().getScheme();
      }
      if (authority == null) {
        authority = dir.toUri().getAuthority();
      }

      if (!scheme.equals(dir.toUri().getScheme())) {
        return Optional.empty();
      }

      // Authority can be null - for example: file:///abc/
      if (authority != null && !authority.equals(dir.toUri().getAuthority())) {
        return Optional.empty();
      }
    }

    String commonDir = null;
    for (Path dir : dirs) {
      String dirPathString = dir.toUri().getPath();
      if (commonDir == null) {
        commonDir = dirPathString;
      } else {
        commonDir = commonDir(commonDir, dirPathString);
      }
    }

    // No common directory
    if (commonDir.length() == 0) {
      return Optional.empty();
    }

    return Optional.of(new Path(scheme, authority, commonDir));
  }

  /**
   * Get the most specific common directory.
   *
   * @param dir1 first directory
   * @param dir2 second directory
   * @return the most specific directory that contains both dir1 and dir2. e.g /a/b/c, /a/d/e => /a
   */
  public static String commonDir(String dir1, String dir2) {
    String[] path1Elements = dir1.split("/");
    String[] path2Elements = dir2.split("/");
    List<String> commonPath = new ArrayList<>();

    int pathIndex = 0;
    while (pathIndex < path1Elements.length && pathIndex < path2Elements.length) {
      if (path1Elements[pathIndex].equals(path2Elements[pathIndex])) {
        commonPath.add(path1Elements[pathIndex]);
      } else {
        break;
      }
      pathIndex++;
    }
    return org.apache.commons.lang.StringUtils.join(commonPath, "/");
  }

  /**
   * Get the locations of the specified partitions.
   *
   * @param partitions partitions to get the locations of
   * @return a set of locations corresponding to the partitions
   */
  public static Set<Path> getLocations(Collection<Partition> partitions) {
    Set<Path> paths = new HashSet<>();
    for (Partition p : partitions) {
      String location = p.getSd().getLocation();
      if (location != null) {
        paths.add(new Path(location));
      }
    }
    return paths;
  }

  public static void exponentialSleep(int attempt) throws InterruptedException {
    exponentialSleep(attempt, DEFAULT_WAIT_TIME_BASE, DEFAULT_MAX_WAIT_TIME);
  }

  /**
   * Sleep for a period of time that relates exponentially to the attempt number.
   *
   * @param attempt attempt number
   * @param base the wait time in ms when attempt is 0
   * @param max the maximum wait time in ms
   *
   * @throws InterruptedException if the waiting thread gets interrupted
   */
  public static void exponentialSleep(int attempt, int base, int max) throws InterruptedException {
    long sleepSeconds = (long) Math.min(max, Math.pow(base, attempt));
    LOG.debug(String.format("Attempt %d: sleeping for %d seconds", attempt, sleepSeconds));
    Thread.sleep(1000 * sleepSeconds);
  }

  /**
   * Returns a string row-like representation of the input columns.
   *
   * @param columns The columns to convert into the row.
   * @return a string row-like representation of the input columns.
   */
  public static String genValue(String... columns) {
    return Joiner.on("\t").useForNull("NULL").join(columns);
  }

  /**
   * Normalizes the DB name and table name in a table object.
   *
   * @param table the table object whose names are to be normalized
   * @return the table object with normalized names
   */
  public static Table normalizeNames(Table table) {
    table.setDbName(table.getDbName().toLowerCase());
    table.setTableName(table.getTableName().toLowerCase());

    return table;
  }

  /**
   * Normalizes the DB name and table name in a table object.
   *
   * @param partition the partition object whose names are to be normalized
   * @return the partition object with normalized names
   */
  public static Partition normalizeNames(Partition partition) {
    partition.setDbName(partition.getDbName().toLowerCase());
    partition.setTableName(partition.getTableName().toLowerCase());

    return partition;
  }


  private static Long getLongValue(Map<String, String> parameters, String key) {
    if (parameters == null) {
      return null;
    }
    String value = parameters.get(key);
    if (value == null) {
      return null;
    }
    try {
      return Long.valueOf(value);
    } catch (NumberFormatException e) {
      LOG.warn("NumberFormatException: value for key " + key + ": " + value
          + " is not a valid Integer.");
      return null;
    }
  }

  private static Long getLastModifiedTime(Map<String, String> parameters) {
    Long lastModifiedTime = getLongValue(parameters, HiveParameterKeys.TLMT);
    Long transientLastDdlTime = getLongValue(parameters, HiveParameterKeys.TLDT);

    if (lastModifiedTime == null) {
      return transientLastDdlTime;
    }
    if (transientLastDdlTime == null) {
      return lastModifiedTime;
    }
    return Long.max(lastModifiedTime, transientLastDdlTime);
  }

  /**
   * Return the last modified time of a table.
   */
  public static Long getLastModifiedTime(Table table) {
    if (table == null) {
      return null;
    }
    Map<String, String> parameters = table.getParameters();
    return getLastModifiedTime(parameters);
  }

  /**
   * Return the last modified time of a partition.
   */
  public static Long getLastModifiedTime(Partition partition) {
    if (partition == null) {
      return null;
    }
    Map<String, String> parameters = partition.getParameters();
    return getLastModifiedTime(parameters);
  }

  /**
   * Return whether the src table is older than the dest table.
   */
  public static boolean isSrcOlder(Table src, Table dest) {
    Long srcModifiedTime = ReplicationUtils.getLastModifiedTime(src);
    Long destModifiedTime = ReplicationUtils.getLastModifiedTime(dest);
    return (srcModifiedTime != null && destModifiedTime != null
        && srcModifiedTime < destModifiedTime);
  }

  /**
   * Return whether the src partition is older than the dest partition.
   */
  public static boolean isSrcOlder(Partition src, Partition dest) {
    Long srcModifiedTime = ReplicationUtils.getLastModifiedTime(src);
    Long destModifiedTime = ReplicationUtils.getLastModifiedTime(dest);
    return (srcModifiedTime != null && destModifiedTime != null
        && srcModifiedTime < destModifiedTime);
  }

}
