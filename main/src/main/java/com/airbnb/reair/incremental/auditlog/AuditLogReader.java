package com.airbnb.reair.incremental.auditlog;

import com.airbnb.reair.common.Container;
import com.airbnb.reair.common.NamedPartition;
import com.airbnb.reair.db.DbConnectionFactory;
import com.airbnb.reair.hive.hooks.HiveOperation;
import com.airbnb.reair.incremental.MetadataException;
import com.airbnb.reair.incremental.ReplicationUtils;
import com.airbnb.reair.incremental.db.DbConstants;
import com.airbnb.reair.incremental.deploy.ConfigurationKeys;
import com.airbnb.reair.utils.RetryableTask;
import com.airbnb.reair.utils.RetryingTaskRunner;

import org.apache.commons.lang.math.LongRange;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;

/**
 * Reads entries from the Hive audit log.
 */
public class AuditLogReader {

  private static final Log LOG = LogFactory.getLog(AuditLogReader.class);

  private static final int ROW_FETCH_SIZE = 200;

  private DbConnectionFactory dbConnectionFactory;
  private String auditLogTableName;
  private String outputObjectsTableName;
  private String mapRedStatsTableName;
  private long lastReadId;
  private Queue<AuditLogEntry> auditLogEntries;
  private RetryingTaskRunner retryingTaskRunner;

  /**
   * Constructs an AuditLogReader.
   *
   * @param dbConnectionFactory factory for creating connections to the DB where the log resides
   * @param auditLogTableName name of the table on the DB that contains the audit log entries
   * @param outputObjectsTableName name of the table on the DB that contains serialized objects
   * @param mapRedStatsTableName name of the table on the DB that contains job stats
   * @param getIdsAfter start reading entries from the audit log after this ID value
   */
  public AuditLogReader(
      Configuration conf,
      DbConnectionFactory dbConnectionFactory,
      String auditLogTableName,
      String outputObjectsTableName,
      String mapRedStatsTableName,
      long getIdsAfter) throws SQLException {
    this.dbConnectionFactory = dbConnectionFactory;
    this.auditLogTableName = auditLogTableName;
    this.outputObjectsTableName = outputObjectsTableName;
    this.mapRedStatsTableName = mapRedStatsTableName;
    this.lastReadId = getIdsAfter;
    auditLogEntries = new LinkedList<>();
    this.retryingTaskRunner = new RetryingTaskRunner(
        conf.getInt(ConfigurationKeys.DB_QUERY_RETRIES,
            DbConstants.DEFAULT_NUM_RETRIES),
        DbConstants.DEFAULT_RETRY_EXPONENTIAL_BASE);
  }

  /**
   * Return the next audit log entry from the DB. If there is an error connecting to the DB, retry.
   *
   * @return the next audit log entry
   *
   * @throws SQLException if there is an error querying the DB
   */
  public synchronized Optional<AuditLogEntry> resilientNext()
      throws AuditLogEntryException, SQLException {
    final Container<Optional<AuditLogEntry>> ret = new Container<>();

    try {
      retryingTaskRunner.runWithRetries(new RetryableTask() {
        @Override
        public void run() throws Exception {
          ret.set(next());
        }
      });
    } catch (SQLException | AuditLogEntryException e) {
      // These should be the only exceptions thrown
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    return ret.get();
  }

  /**
   * Return the next audit log entry from the DB.
   *
   * @return the next audit log entry
   *
   * @throws SQLException if there is an error querying the DB
   * @throws AuditLogEntryException if there is an error reading the audit log entry
   */
  public synchronized Optional<AuditLogEntry> next() throws SQLException, AuditLogEntryException {
    if (auditLogEntries.size() > 0) {
      return Optional.of(auditLogEntries.remove());
    }

    LOG.debug("Executing queries to try to get more audit log entries from the DB");

    fetchMoreEntries();

    if (auditLogEntries.size() > 0) {
      return Optional.of(auditLogEntries.remove());
    } else {
      return Optional.empty();
    }
  }

  /**
   * From the output column in the audit log table, return the partition name. An example is
   * "default.table/ds=1" => "ds=1".
   *
   * @param outputCol the output column in the audit log table
   * @return the partition name
   */
  private String getPartitionNameFromOutputCol(String outputCol) {
    return outputCol.substring(outputCol.indexOf("/") + 1);
  }

  private HiveOperation convertToHiveOperation(String operation) {
    if (operation == null) {
      return null;
    }

    try {
      return HiveOperation.valueOf(operation);
    } catch (IllegalArgumentException e) {
      return null;
    }
  }

  /**
   * Given that we start reading after lastReadId and need to get
   * ROW_FETCH_SIZE rows from the audit log, figure out the min and max row
   * IDs to read.
   *
   * @returns a range of ID's to read from the audit log table based on the fetch size
   * @throws SQLException if there is an error reading from the DB
   */
  private LongRange getIdsToRead() throws SQLException {
    String queryFormatString = "SELECT MIN(id) min_id, MAX(id) max_id "
        + "FROM (SELECT id FROM %s WHERE id > %s "
        + "AND (command_type IS NULL OR command_type NOT IN('SHOWTABLES', 'SHOWPARTITIONS', "
        + "'SWITCHDATABASE')) "
        + "ORDER BY id "
        + "LIMIT %s)"
        + " subquery "
        // Get read locks on the specified rows to prevent skipping of rows that haven't committed
        // yet, but have an id that matches the where clause.
        // For example, one transaction starts and
        // inserts id = 1, but another transaction starts, inserts, and commits i = 2 before the
        // first transaction commits. Locking can also be done with serializable isolation level.
        + "LOCK IN SHARE MODE";
    String query = String.format(queryFormatString, auditLogTableName, lastReadId, ROW_FETCH_SIZE);
    Connection connection = dbConnectionFactory.getConnection();

    PreparedStatement ps = connection.prepareStatement(query);
    LOG.debug("Executing: " + query);
    ResultSet rs = ps.executeQuery();
    if (rs.next()) {
      long minId = rs.getLong("min_id");
      long maxId = rs.getLong("max_id");
      return new LongRange(minId, maxId);
    }
    return new LongRange(0, 0);
  }


  private void fetchMoreEntries() throws SQLException, AuditLogEntryException {

    LongRange idsToRead = getIdsToRead();

    // No more entries to read
    if (idsToRead.getMaximumLong() == 0) {
      return;
    }

    // TODO: Remove left outer join and command type filter once the
    // exchange partition bug is fixed in HIVE-12215
    String queryFormatString = "SELECT a.id, a.create_time, "
        + "command_type, command, name, category, "
        + "type, serialized_object "
        + "FROM %s a LEFT OUTER JOIN %s b on a.id = b.audit_log_id "
        + "WHERE a.id >= ? AND a.id <= ? "
        + "AND (command_type IS NULL OR command_type "
        + "NOT IN('SHOWTABLES', 'SHOWPARTITIONS', 'SWITCHDATABASE')) "
        + "ORDER BY id "
        // Get read locks on the specified rows to prevent skipping of rows that haven't committed
        // yet, but have an ID between idsToRead. For example, one transaction starts and
        // inserts id = 1, but another transaction starts, inserts, and commits i=2 before the
        // first transaction commits. Locking can also be done with serializable isolation level.
        + "LOCK IN SHARE MODE";
    String query = String.format(queryFormatString,
        auditLogTableName, outputObjectsTableName,
        idsToRead.getMinimumLong(), idsToRead.getMaximumLong());

    Connection connection = dbConnectionFactory.getConnection();
    PreparedStatement ps = connection.prepareStatement(query);

    int index = 1;
    ps.setLong(index++, idsToRead.getMinimumLong());
    ps.setLong(index++, idsToRead.getMaximumLong());

    ResultSet rs = ps.executeQuery();

    long id = -1;
    Timestamp createTime = null;
    HiveOperation commandType = null;
    String command = null;
    String objectName;
    String objectCategory;
    String objectType;
    String objectSerialized;

    long previouslyReadId = -1;
    Timestamp previouslyReadTs = null;
    HiveOperation previousCommandType = null;
    String previousCommand = null;


    // For a given audit log ID, the join would have produced multiple rows
    // for each ID. Each row contains a single output. Group all the rows
    // and the outputs into a AuditLogEntry.

    // For a given audit log ID, these accumulate the outputs from the
    // different rows.
    List<String> outputDirectories = new LinkedList<>();
    List<Table> outputTables = new LinkedList<>();
    List<NamedPartition> outputPartitions = new LinkedList<>();
    List<Table> referenceTables = new LinkedList<>();
    Table inputTable = null;
    NamedPartition renameFromPartition = null;

    while (rs.next()) {
      id = rs.getLong("id");
      createTime = rs.getTimestamp("create_time");
      // Invalid operations are returned as null
      String commandTypeString = rs.getString("command_type");
      commandType = convertToHiveOperation(commandTypeString);
      if (commandType == null) {
        LOG.debug(String.format("Invalid operation %s in audit log id: %s", commandTypeString, id));
      }
      command = rs.getString("command");
      objectName = rs.getString("name");
      objectCategory = rs.getString("category");
      objectType = rs.getString("type");
      objectSerialized = rs.getString("serialized_object");

      if (previouslyReadId != -1 && id != previouslyReadId) {
        lastReadId = previouslyReadId;
        // This means that all the outputs for a given audit log entry
        // has been read.
        AuditLogEntry entry = new AuditLogEntry(
            previouslyReadId,
            previouslyReadTs,
            previousCommandType,
            previousCommand,
            outputDirectories,
            referenceTables,
            outputTables,
            outputPartitions,
            inputTable,
            renameFromPartition);
        auditLogEntries.add(entry);
        // Reset these accumulated values
        outputDirectories = new LinkedList<>();
        referenceTables = new LinkedList<>();
        outputTables = new LinkedList<>();
        outputPartitions = new LinkedList<>();
        renameFromPartition = null;
        inputTable = null;
      }

      previouslyReadId = id;
      previouslyReadTs = createTime;
      previousCommandType = commandType;
      previousCommand = command;

      if ("DIRECTORY".equals(objectType)) {
        outputDirectories.add(objectName);
      } else if ("TABLE".equals(objectType)) {
        Table table = new Table();
        try {
          ReplicationUtils.deserializeObject(objectSerialized, table);
        } catch (MetadataException e) {
          throw new AuditLogEntryException(e);
        }
        ReplicationUtils.normalizeNames(table);
        if ("OUTPUT".equals(objectCategory)) {
          outputTables.add(table);
        } else if ("REFERENCE_TABLE".equals(objectCategory)) {
          referenceTables.add(table);
        } else if ("RENAME_FROM".equals(objectCategory) || "INPUT".equals(objectCategory)) {
          inputTable = table;
        } else {
          throw new RuntimeException("Unhandled category: " + objectCategory);
        }
      } else if ("PARTITION".equals(objectType)) {
        Partition partition = new Partition();

        try {
          ReplicationUtils.deserializeObject(objectSerialized, partition);
        } catch (MetadataException e) {
          throw new AuditLogEntryException(e);
        }
        ReplicationUtils.normalizeNames(partition);
        String partitionName = getPartitionNameFromOutputCol(objectName);
        NamedPartition namedPartition = new NamedPartition(partitionName, partition);

        if ("OUTPUT".equals(objectCategory)) {
          outputPartitions.add(namedPartition);
        } else if ("RENAME_FROM".equals(objectCategory) || "INPUT".equals(objectCategory)) {
          renameFromPartition = namedPartition;
        } else {
          throw new RuntimeException("Unhandled category: " + objectCategory);
        }
      } else if ("DFS_DIR".equals(objectType)) {
        outputDirectories.add(objectName);
      } else if ("LOCAL_DIR".equals(objectType)) {
        outputDirectories.add(objectName);
      } else if ("DATABASE".equals(objectType)) {
        // Currently, nothing is done with DB's
      } else if (objectType == null) {
        // This will happen for queries that don't have any output
        // objects. This can be removed a long with the OUTER aspect
        // of the join above once the bug with exchange partitions is
        // fixed.
        LOG.debug("No output objects");
      } else {
        throw new RuntimeException("Unhandled output type: " + objectType);
      }
    }
    
    // This is the case where we read to the end of the table.
    if (id != -1) {
      AuditLogEntry entry = new AuditLogEntry(
          id,
          createTime,
          commandType,
          command,
          outputDirectories,
          referenceTables,
          outputTables,
          outputPartitions,
          inputTable,
          renameFromPartition);
      auditLogEntries.add(entry);
    }
    // Note: if we constantly get empty results (i.e. no valid entries
    // because all the commands got filtered out), then the lastReadId won't
    // be updated for a while.
    lastReadId = idsToRead.getMaximumLong();
    return;
  }

  /**
   * Change the reader to start reading entries after this ID.
   *
   * @param readAfterId ID to configure the reader to read after
   */
  public synchronized void setReadAfterId(long readAfterId) {
    this.lastReadId = readAfterId;
    // Clear the audit log entries since it's possible that the reader
    // fetched a bunch of entries in advance, and the ID of those entries
    // may not line up with the new read-after ID.
    auditLogEntries.clear();
  }

  /**
   * Return the highest value ID in the audit log table.
   *
   * @return the highest value ID in the audit log table or empty if it does not exist
   * @throws SQLException if there an exception reading from the DB
   */
  public synchronized Optional<Long> getMaxId() throws SQLException {
    String query = String.format("SELECT MAX(id) FROM %s", auditLogTableName);
    Connection connection = dbConnectionFactory.getConnection();
    PreparedStatement ps = connection.prepareStatement(query);

    ResultSet rs = ps.executeQuery();

    rs.next();
    return Optional.ofNullable(rs.getLong(1));
  }
}
