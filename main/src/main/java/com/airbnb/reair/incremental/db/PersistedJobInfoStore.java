package com.airbnb.reair.incremental.db;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import com.airbnb.reair.db.DbConnectionFactory;
import com.airbnb.reair.incremental.ReplicationOperation;
import com.airbnb.reair.incremental.ReplicationStatus;
import com.airbnb.reair.incremental.ReplicationUtils;
import com.airbnb.reair.incremental.StateUpdateException;
import com.airbnb.reair.incremental.deploy.ConfigurationKeys;
import com.airbnb.reair.utils.RetryableTask;
import com.airbnb.reair.utils.RetryingTaskRunner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * A store for managing and persisting PersistedJobInfo objects. The objects are stored though a
 * state table that generally has a separate column for each field in PersistedJobInfo. This avoids
 * the use of ORM as the use case is relatively simple.
 *
 * <p>Note: to simplify programming, all methods are synchronized. This could be slow, so another
 * approach is for each thread to use a different DB connection for higher parallelism.
 */
public class PersistedJobInfoStore {

  private static final Log LOG = LogFactory.getLog(PersistedJobInfoStore.class);

  private static final String[] completedStateStrings = {
      ReplicationStatus.SUCCESSFUL.name(),
      ReplicationStatus.FAILED.name(),
      ReplicationStatus.NOT_COMPLETABLE.name(),
      ReplicationStatus.ABORTED.name()};

  private DbConnectionFactory dbConnectionFactory;
  private String dbTableName;
  private RetryingTaskRunner retryingTaskRunner = new RetryingTaskRunner();

  /**
   * Constructor.
   *
   * @param conf configuration
   * @param dbConnectionFactory factory for creating connections to the DB
   * @param dbTableName name of the table on the DB that stores job information
   */
  public PersistedJobInfoStore(Configuration conf,
                               DbConnectionFactory dbConnectionFactory,
                               String dbTableName) {
    this.dbConnectionFactory = dbConnectionFactory;
    this.dbTableName = dbTableName;
    this.retryingTaskRunner = new RetryingTaskRunner(
        conf.getInt(ConfigurationKeys.DB_QUERY_RETRIES,
            DbConstants.DEFAULT_NUM_RETRIES),
        DbConstants.DEFAULT_RETRY_EXPONENTIAL_BASE);
  }

  /**
   * Make the `create table` statement that can be run on the DB to create the table containing the
   * information required for a PersistedJobInfo object.
   *
   * @param tableName the table name to use in the DB
   * @return a SQL command that could be executed to create the state table
   */
  public static String getCreateTableSql(String tableName) {
    return String.format("CREATE TABLE `%s` (\n" + "  `id` bigint(20) NOT NULL AUTO_INCREMENT,\n"
        + "  `create_time` timestamp DEFAULT 0, \n"
        + "  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON "
        + "       UPDATE CURRENT_TIMESTAMP,\n"
        + "  `operation` varchar(256) DEFAULT NULL,\n" + "  `status` varchar(4000) DEFAULT NULL,\n"
        + "  `src_path` varchar(4000) DEFAULT NULL,\n"
        + "  `src_cluster` varchar(256) DEFAULT NULL,\n"
        + "  `src_db` varchar(4000) DEFAULT NULL,\n" + "  `src_table` varchar(4000) DEFAULT NULL,\n"
        + "  `src_partitions` mediumtext DEFAULT NULL,\n"
        + "  `src_tldt` varchar(4000) DEFAULT NULL,\n"
        + "  `rename_to_db` varchar(4000) DEFAULT NULL,\n"
        + "  `rename_to_table` varchar(4000) DEFAULT NULL,\n"
        + "  `rename_to_partition` varchar(4000) DEFAULT NULL,\n"
        + "  `rename_to_path` varchar(4000), \n" + "  `extras` mediumtext, \n"
        + "  PRIMARY KEY (`id`),\n" + "  KEY `update_time_index` (`update_time`),\n"
        + "  KEY `src_cluster_index` (`src_cluster`),\n" + "  KEY `src_db_index` (`src_db`(767)),\n"
        + "  KEY `src_table_index` (`src_table`(767))\n" + ") ENGINE=InnoDB", tableName);
  }

  /**
   * Changes the state for all jobs that are not finished to ABORTED.
   *
   * @throws SQLException if there is an error querying the DB
   */
  public synchronized void abortRunnableFromDb() throws SQLException {
    // Convert from ['a', 'b'] to "'a', 'b'"
    String completedStateList = StringUtils.join(", ",
        Lists.transform(Arrays.asList(completedStateStrings), new Function<String, String>() {
          public String apply(String str) {
            return String.format("'%s'", str);
          }
        }));
    String query = String.format("UPDATE %s SET status = 'ABORTED' " + "WHERE status NOT IN (%s)",
        dbTableName, completedStateList);
    Connection connection = dbConnectionFactory.getConnection();
    Statement statement = connection.createStatement();
    statement.execute(query);
  }

  /**
   * Gets all the jobs that have a not completed status (and therefore should be run) from the DB.
   *
   * @return a list of jobs in the DB that should be run
   * @throws SQLException if there's an error querying the DB
   */
  public synchronized List<PersistedJobInfo> getRunnableFromDb() throws SQLException {
    // Convert from ['a', 'b'] to "'a', 'b'"
    String completedStateList = StringUtils.join(", ",
        Lists.transform(Arrays.asList(completedStateStrings), new Function<String, String>() {
          public String apply(String str) {
            return String.format("'%s'", str);
          }
        }));
    String query = String.format("SELECT id, create_time, operation, status, src_path, "
        + "src_cluster, src_db, "
        + "src_table, src_partitions, src_tldt, "
        + "rename_to_db, rename_to_table, rename_to_partition, "
        + "rename_to_path, extras "
        + "FROM %s WHERE status NOT IN (%s) ORDER BY id", dbTableName, completedStateList);

    List<PersistedJobInfo> persistedJobInfos = new ArrayList<>();
    Connection connection = dbConnectionFactory.getConnection();

    Statement statement = connection.createStatement();
    ResultSet rs = statement.executeQuery(query);

    while (rs.next()) {
      long id = rs.getLong("id");
      Optional<Timestamp> createTimestamp = Optional.ofNullable(rs.getTimestamp("create_time"));
      long createTime = createTimestamp.map(Timestamp::getTime).orElse(Long.valueOf(0));
      ReplicationOperation operation = ReplicationOperation.valueOf(rs.getString("operation"));
      ReplicationStatus status = ReplicationStatus.valueOf(rs.getString("status"));
      Optional srcPath = Optional.ofNullable(rs.getString("src_path")).map(Path::new);
      String srcClusterName = rs.getString("src_cluster");
      String srcDbName = rs.getString("src_db");
      String srcTableName = rs.getString("src_table");
      List<String> srcPartitionNames = new ArrayList<>();
      String partitionNamesJson = rs.getString("src_partitions");
      if (partitionNamesJson != null) {
        srcPartitionNames = ReplicationUtils.convertToList(partitionNamesJson);
      }
      Optional<String> srcObjectTldt = Optional.ofNullable(rs.getString("src_tldt"));
      Optional<String> renameToDbName = Optional.ofNullable(rs.getString("rename_to_db"));
      Optional<String> renameToTableName = Optional.ofNullable(rs.getString("rename_to_table"));
      Optional<String> renameToPartitionName =
          Optional.ofNullable(rs.getString("rename_to_partition"));
      Optional<Path> renameToPath =
          Optional.ofNullable(rs.getString("rename_to_path")).map(Path::new);
      Optional<String> extrasJson = Optional.ofNullable(rs.getString("extras"));
      Map<String, String> extras =
          extrasJson.map(ReplicationUtils::convertToMap).orElse(new HashMap<>());

      PersistedJobInfo persistedJobInfo = new PersistedJobInfo(Optional.of(id), createTime,
          operation, status, srcPath, srcClusterName, srcDbName, srcTableName, srcPartitionNames,
          srcObjectTldt, renameToDbName, renameToTableName, renameToPartitionName, renameToPath,
          extras);
      persistedJobInfos.add(persistedJobInfo);
    }
    return persistedJobInfos;
  }

  private synchronized void persistHelper(PersistedJobInfo job) throws SQLException, IOException {
    String query = "INSERT INTO " + dbTableName
        + " SET " + "id = ?, " + "create_time = ?, "
        + "operation = ?, " + "status = ?, "
        + "src_path = ?, " + "src_cluster = ?, "
        + "src_db = ?, " + "src_table = ?, "
        + "src_partitions = ?, " + "src_tldt = ?, "
        + "rename_to_db = ?, " + "rename_to_table = ?, "
        + "rename_to_partition = ?, "
        + "rename_to_path = ?, "
        + "extras = ? "
        + "ON DUPLICATE KEY UPDATE " + "create_time = ?, "
        + "operation = ?, "
        + "status = ?, "
        + "src_path = ?, "
        + "src_cluster = ?, "
        + "src_db = ?, "
        + "src_table = ?, "
        + "src_partitions = ?, "
        + "src_tldt = ?, "
        + "rename_to_db = ?, "
        + "rename_to_table = ?, "
        + "rename_to_partition = ?, "
        + "rename_to_path = ?, "
        + "extras = ?";

    Connection connection = dbConnectionFactory.getConnection();
    PreparedStatement ps = connection.prepareStatement(query);
    try {
      int queryParamIndex = 1;
      ps.setLong(queryParamIndex++, job.getId());
      ps.setTimestamp(queryParamIndex++, new Timestamp(job.getCreateTime()));
      ps.setString(queryParamIndex++, job.getOperation().toString());
      ps.setString(queryParamIndex++, job.getStatus().toString());
      ps.setString(queryParamIndex++, job.getSrcPath().map(Path::toString).orElse(null));
      ps.setString(queryParamIndex++, job.getSrcClusterName());
      ps.setString(queryParamIndex++, job.getSrcDbName());
      ps.setString(queryParamIndex++, job.getSrcTableName());
      ps.setString(queryParamIndex++, ReplicationUtils.convertToJson(job.getSrcPartitionNames()));
      ps.setString(queryParamIndex++, job.getSrcObjectTldt().orElse(null));
      ps.setString(queryParamIndex++, job.getRenameToDb().orElse(null));
      ps.setString(queryParamIndex++, job.getRenameToTable().orElse(null));
      ps.setString(queryParamIndex++, job.getRenameToPartition().orElse(null));
      ps.setString(queryParamIndex++, job.getRenameToPath().map(Path::toString).orElse(null));
      ps.setString(queryParamIndex++, ReplicationUtils.convertToJson(job.getExtras()));

      // Handle the update case
      ps.setTimestamp(queryParamIndex++, new Timestamp(job.getCreateTime()));
      ps.setString(queryParamIndex++, job.getOperation().toString());
      ps.setString(queryParamIndex++, job.getStatus().toString());
      ps.setString(queryParamIndex++, job.getSrcPath().map(Path::toString).orElse(null));
      ps.setString(queryParamIndex++, job.getSrcClusterName());
      ps.setString(queryParamIndex++, job.getSrcDbName());
      ps.setString(queryParamIndex++, job.getSrcTableName());
      ps.setString(queryParamIndex++, ReplicationUtils.convertToJson(job.getSrcPartitionNames()));
      ps.setString(queryParamIndex++, job.getSrcObjectTldt().orElse(null));
      ps.setString(queryParamIndex++, job.getRenameToDb().orElse(null));
      ps.setString(queryParamIndex++, job.getRenameToTable().orElse(null));
      ps.setString(queryParamIndex++, job.getRenameToPartition().orElse(null));
      ps.setString(queryParamIndex++, job.getRenameToPath().map(Path::toString).orElse(null));
      ps.setString(queryParamIndex++, ReplicationUtils.convertToJson(job.getExtras()));

      ps.execute();
    } finally {
      ps.close();
      ps = null;
    }
  }

  public synchronized void changeStatusAndPersist(ReplicationStatus status, PersistedJobInfo job)
      throws StateUpdateException {
    job.setStatus(status);
    persist(job);
  }

  /**
   * Persist the data from the job into the DB.
   *
   * @param job the job to persist
   */
  public synchronized void persist(final PersistedJobInfo job) throws StateUpdateException {
    try {
      retryingTaskRunner.runWithRetries(new RetryableTask() {
        @Override
        public void run() throws Exception {
          persistHelper(job);
        }
      });
    } catch (IOException | SQLException e) {
      throw new StateUpdateException(e);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private synchronized void createManyImpl(List<PersistedJobInfo> jobs)
      throws IOException, SQLException, StateUpdateException {
    LOG.debug(String.format("Persisting %d PersistedJobInfos", jobs.size()));
    if (jobs.size() == 0) {
      return;
    }
    String query = generateQuery(jobs.size());
    Connection connection = dbConnectionFactory.getConnection();
    try (PreparedStatement ps =
             connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
      int queryParamIndex = 1;
      for (PersistedJobInfo job: jobs) {
        ps.setTimestamp(queryParamIndex++, new Timestamp(job.getCreateTime()));
        ps.setString(queryParamIndex++, job.getOperation().toString());
        ps.setString(queryParamIndex++, job.getStatus().toString());
        ps.setString(queryParamIndex++, job.getSrcPath().map(Path::toString).orElse(null));
        ps.setString(queryParamIndex++, job.getSrcClusterName());
        ps.setString(queryParamIndex++, job.getSrcDbName());
        ps.setString(queryParamIndex++, job.getSrcTableName());
        ps.setString(queryParamIndex++, ReplicationUtils.convertToJson(job.getSrcPartitionNames()));
        ps.setString(queryParamIndex++, job.getSrcObjectTldt().orElse(null));
        ps.setString(queryParamIndex++, job.getRenameToDb().orElse(null));
        ps.setString(queryParamIndex++, job.getRenameToTable().orElse(null));
        ps.setString(queryParamIndex++, job.getRenameToPartition().orElse(null));
        ps.setString(queryParamIndex++, job.getRenameToPath().map(Path::toString).orElse(null));
        ps.setString(queryParamIndex++, ReplicationUtils.convertToJson(job.getExtras()));
      }
      ps.execute();
      ResultSet rs = ps.getGeneratedKeys();
      for (PersistedJobInfo j : jobs) {
        rs.next();
        j.setPersisted(rs.getLong(1));
      }
    }
  }

  /**
   * Persists PENDING PersistedJobInfos to the DB.
   * @param jobs a list of PersistedJobInfos in the PENDING state
   * @throws StateUpdateException if there is a SQLException or any Infos are not PENDING
   */
  public synchronized void createMany(List<PersistedJobInfo> jobs)
      throws StateUpdateException {
    for (PersistedJobInfo job: jobs) {
      if (job.getPersistState() == PersistedJobInfo.PersistState.PERSISTED) {
        throw new StateUpdateException("Tried to persist already persisted PersistedJobInfo.");
      }
    }
    try {
      retryingTaskRunner.runWithRetries(() -> createManyImpl(jobs));
    } catch (IOException | SQLException e) {
      throw new StateUpdateException(e);
    } catch (StateUpdateException e) {
      throw e;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private String generateQuery(int len) {
    String valuesStr = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    StringBuffer sb = new StringBuffer();
    sb.append(
        "INSERT INTO " + dbTableName + " (create_time, operation, status, src_path, "
            + "src_cluster, src_db, src_table, src_partitions, src_tldt, rename_to_db, "
            + "rename_to_table, rename_to_partition, rename_to_path, extras) VALUES ");
    for (int i = 1; i < len; i++) {
      sb.append(valuesStr);
      sb.append(" , ");
    }
    sb.append(valuesStr);
    return sb.toString();
  }

  private synchronized PersistedJobInfo getJob(long id) throws SQLException {
    String query = "SELECT id, create_time, operation, status, src_path, " + "src_cluster, src_db, "
        + "src_table, src_partitions, src_tldt, "
        + "rename_to_db, rename_to_table, rename_to_partition, " + "rename_to_path, extras "
        + "FROM " + dbTableName + " WHERE id = ?";

    Connection connection = dbConnectionFactory.getConnection();

    PreparedStatement ps = connection.prepareStatement(query);
    ResultSet rs = ps.executeQuery(query);

    while (rs.next()) {
      Optional<Timestamp> ts = Optional.ofNullable(rs.getTimestamp("create_time"));
      long createTime = ts.map(Timestamp::getTime).orElse(Long.valueOf(0));
      ReplicationOperation operation = ReplicationOperation.valueOf(rs.getString("operation"));
      ReplicationStatus status = ReplicationStatus.valueOf(rs.getString("status"));
      Optional<Path> srcPath = Optional.ofNullable(rs.getString("src_path")).map(Path::new);
      String srcClusterName = rs.getString("src_cluster");
      String srcDbName = rs.getString("src_db");
      String srcTableName = rs.getString("src_table");
      List<String> srcPartitionNames = new ArrayList<>();
      String partitionNamesJson = rs.getString("src_partitions");
      if (partitionNamesJson != null) {
        srcPartitionNames = ReplicationUtils.convertToList(partitionNamesJson);
      }
      Optional<String> srcObjectTldt = Optional.of(rs.getString("src_tldt"));
      Optional<String> renameToDbName = Optional.of(rs.getString("rename_to_db"));
      Optional<String> renameToTableName = Optional.of(rs.getString("rename_to_table"));
      Optional<String> renameToPartitionName = Optional.of(rs.getString("rename_to_partition"));
      Optional<Path> renameToPath = Optional.of(rs.getString("rename_to_path")).map(Path::new);
      String extrasJson = rs.getString("extras");
      Map<String, String> extras = new HashMap<>();
      if (extrasJson != null) {
        extras = ReplicationUtils.convertToMap(rs.getString("extras"));
      }

      PersistedJobInfo persistedJobInfo = new PersistedJobInfo(Optional.of(id), createTime,
          operation, status, srcPath, srcClusterName, srcDbName, srcTableName, srcPartitionNames,
          srcObjectTldt, renameToDbName, renameToTableName, renameToPartitionName, renameToPath,
          extras);
      return persistedJobInfo;
    }
    return null;
  }
}
