package com.airbnb.reair.incremental.db;

import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.db.DbConnectionFactory;
import com.airbnb.reair.incremental.ReplicationJob;
import com.airbnb.reair.incremental.ReplicationOperation;
import com.airbnb.reair.incremental.ReplicationStatus;
import com.airbnb.reair.incremental.ReplicationUtils;
import com.airbnb.reair.incremental.StateUpdateException;

import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

public class PersistedJobInfoCreator {
  private ArrayList<QueryParams> vars;
  private ArrayList<CompletableFuture<Long>> futures;
  private DbConnectionFactory dbConnectionFactory;
  private String dbTableName;

  private static final int BATCH_SIZE = 32;

  /**
   * Creates rows for PersistedJobInfo in batches. You can add to the batch
   * with createLater, and finalize with completeFutures.
   *
   * @param dbConnectionFactory DbConnectionFactory for state table
   * @param dbTableName Name of state table
   */
  public PersistedJobInfoCreator(DbConnectionFactory dbConnectionFactory, String dbTableName) {
    vars = new ArrayList<>(BATCH_SIZE);
    futures = new ArrayList<>(BATCH_SIZE);
    this.dbConnectionFactory = dbConnectionFactory;
    this.dbTableName = dbTableName;
  }

  /**
   * Returns a CompletableFuture for PersistedJobInfo. The future is
   * completed when completeFutures is called.
   *
   * @param timestampMillisRounded Timestamp in millis rounded to nearest second
   * @param operation the hive operation
   * @param status the status to be used
   * @param srcPath the source path
   * @param srcClusterName the source cluster name
   * @param srcTableSpec the source table spec
   * @param srcPartitionNames the source partition names
   * @param srcTldt the source TLDT
   * @param renameToObject whether to rename or not
   * @param renameToPath whether to rename or not
   * @param extras a map of extras
   * @return A CompletableFuture of the PersistedJobInfo
   * @throws StateUpdateException when there is a SQL error
   */
  public CompletableFuture<PersistedJobInfo> createLater(
      long timestampMillisRounded,
      ReplicationOperation operation,
      ReplicationStatus status,
      Optional<Path> srcPath,
      String srcClusterName,
      HiveObjectSpec srcTableSpec,
      List<String> srcPartitionNames,
      Optional<String> srcTldt,
      Optional<HiveObjectSpec> renameToObject,
      Optional<Path> renameToPath,
      Map<String, String> extras) throws StateUpdateException {
    try {
      QueryParams qp = new QueryParams(
          timestampMillisRounded,
          operation,
          status,
          srcPath,
          srcClusterName,
          srcTableSpec,
          srcPartitionNames,
          srcTldt,
          renameToObject,
          renameToPath,
          extras);
      vars.add(qp);
      CompletableFuture<Long> longCompletableFuture = new CompletableFuture<>();
      futures.add(longCompletableFuture);
      Function<Long, PersistedJobInfo> creationFunction = (Long id) ->
          new PersistedJobInfo(id, timestampMillisRounded, operation, status, srcPath,
              srcClusterName, srcTableSpec.getDbName(), srcTableSpec.getTableName(),
              srcPartitionNames, srcTldt,
              renameToObject.map(HiveObjectSpec::getDbName),
              renameToObject.map(HiveObjectSpec::getTableName),
              renameToObject.map(HiveObjectSpec::getPartitionName),
              renameToPath, extras);
      return longCompletableFuture.thenApplyAsync(creationFunction);
    } catch (IOException e) {
      throw new StateUpdateException(e);
    }
  }

  /**
   * Complete all existing futures in the buffer, creating
   * a SQL entry for each.
   *
   * @throws SQLException if there is a SQL issue
   */
  public void completeFutures() throws SQLException {
    if (futures.size() == 0) {
      return;
    }
    String query = generateQuery();
    Connection connection = dbConnectionFactory.getConnection();
    try (PreparedStatement ps =
             connection.prepareStatement(query, Statement.RETURN_GENERATED_KEYS)) {
      int queryParamIndex = 1;
      for (QueryParams queryParams : vars) {
        ps.setTimestamp(queryParamIndex++, queryParams.timestamp);
        ps.setString(queryParamIndex++, queryParams.operation);
        ps.setString(queryParamIndex++, queryParams.status);
        ps.setString(queryParamIndex++, queryParams.srcPath);
        ps.setString(queryParamIndex++, queryParams.srcClusterName);
        ps.setString(queryParamIndex++, queryParams.srcTableSpecDbName);
        ps.setString(queryParamIndex++, queryParams.srcTableSpecTableName);
        ps.setString(queryParamIndex++, queryParams.srcPartitionNames);
        ps.setString(queryParamIndex++, queryParams.srcTldt);
        ps.setString(queryParamIndex++, queryParams.renameToObjectDbName);
        ps.setString(queryParamIndex++, queryParams.renameToObjectTableName);
        ps.setString(queryParamIndex++, queryParams.renameToObjectPartitionName);
        ps.setString(queryParamIndex++, queryParams.renameToPath);
        ps.setString(queryParamIndex++, queryParams.extras);
      }
      ps.execute();
      ResultSet rs = ps.getGeneratedKeys();
      for (CompletableFuture<Long> f : futures) {
        rs.next();
        f.complete(rs.getLong(1));
      }
    }
    vars.clear();
    futures.clear();
  }

  private String generateQuery() {
    String valuesStr = "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
    StringBuffer sb = new StringBuffer();
    sb.append(
        "INSERT INTO " + dbTableName + " (create_time, operation, status, src_path, "
        + "src_cluster, src_db, src_table, src_partitions, src_tldt, rename_to_db, "
        + "rename_to_table, rename_to_partition, rename_to_path, extras) VALUES ");
    for (int i = 1; i < vars.size(); i++) {
      sb.append(valuesStr);
      sb.append(" , ");
    }
    sb.append(valuesStr);
    return sb.toString();
  }

  private class QueryParams {
    Timestamp timestamp;
    String operation;
    String status;
    String srcPath;
    String srcClusterName;
    String srcTableSpecDbName;
    String srcTableSpecTableName;
    String srcPartitionNames;
    String srcTldt;
    String renameToObjectDbName;
    String renameToObjectTableName;
    String renameToObjectPartitionName;
    String renameToPath;
    String extras;

    public QueryParams(
        long timestampMillisRounded,
        ReplicationOperation operation,
        ReplicationStatus status,
        Optional<Path> srcPath,
        String srcClusterName,
        HiveObjectSpec srcTableSpec,
        List<String> srcPartitionNames,
        Optional<String> srcTldt,
        Optional<HiveObjectSpec> renameToObject,
        Optional<Path> renameToPath,
        Map<String, String> extras) throws IOException {
      this.timestamp = new Timestamp(timestampMillisRounded);
      this.operation = operation.toString();
      this.status = status.toString();
      this.srcPath = srcPath.map(Path::toString).orElse(null);
      this.srcClusterName = srcClusterName;
      this.srcTableSpecDbName = srcTableSpec.getDbName();
      this.srcTableSpecTableName = srcTableSpec.getTableName();
      this.srcPartitionNames = ReplicationUtils.convertToJson(srcPartitionNames);
      this.srcTldt = srcTldt.orElse(null);
      this.renameToObjectDbName = renameToObject.map(HiveObjectSpec::getDbName).orElse(null);
      this.renameToObjectTableName =
          renameToObject.map(HiveObjectSpec::getTableName).orElse(null);
      this.renameToObjectPartitionName =
          renameToObject.map(HiveObjectSpec::getPartitionName).orElse(null);
      this.renameToPath = renameToPath.map(Path::toString).orElse(null);
      this.extras = ReplicationUtils.convertToJson(extras);
    }
  }

}
