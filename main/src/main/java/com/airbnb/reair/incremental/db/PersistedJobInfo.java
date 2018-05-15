package com.airbnb.reair.incremental.db;

import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.incremental.ReplicationOperation;
import com.airbnb.reair.incremental.ReplicationStatus;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

/**
 * Information about a replication job that gets persisted to a DB.
 */
public class PersistedJobInfo {
  public enum PersistState {
    PERSISTED, // if job is created in DB
    PENDING // job hasnt been created yet in DB and has no ID
  }

  private static final Log LOG = LogFactory.getLog(PersistedJobInfo.class);

  private CompletableFuture<Long> id;
  private long createTime;
  private ReplicationOperation operation;
  private ReplicationStatus status;

  // Path of the source may not exist for views
  private Optional<Path> srcPath;
  private String srcClusterName;
  private String srcDbName;
  private String srcTableName;

  // If copying partition(s), the partition names
  private List<String> srcPartitionNames;

  // The modified time of the source object - from the field in parameters,
  // transientLast_ddlTime. This field is only applicable for rename and
  // drop operations.
  private Optional<String> srcObjectTldt;

  // These fields are only applicable for the rename operation.
  private Optional<String> renameToDb;
  private Optional<String> renameToTable;
  private Optional<String> renameToPartition;
  private Optional<Path> renameToPath;

  // A flexible map to store some extra parameters
  private Map<String, String> extras;

  private PersistState persistState;

  public static final String AUDIT_LOG_ID_EXTRAS_KEY = "audit_log_id";
  public static final String AUDIT_LOG_ENTRY_CREATE_TIME_KEY = "audit_log_entry_create_time";
  public static final String BYTES_COPIED_KEY = "bytes_copied";

  /**
   * Constructor for a persisted job info.
   *
   * @param id unique ID for this job
   * @param createTime time that the job was created in millis (rounded to nearest 1000)
   * @param operation the type of operation that the job performs
   * @param status the status of the job
   * @param srcPath the path of the source object
   * @param srcClusterName the name of the source cluster
   * @param srcDbName the name of the source database
   * @param srcTableName the name of the source table
   * @param srcPartitionNames the names of the source partitions
   * @param srcObjectTldt the source object's last modified time (transient_lastDdlTime in
   *                      the parameters field of the Hive Thrift object)
   * @param renameToDb if renaming an object, the new database name
   * @param renameToTable if renaming an object, the new table name
   * @param renameToPartition if renaming an object, the new partition name
   * @param renameToPath if renaming an object, the new object's new location
   * @param extras a key value map of any extra information that is not critical to replication
   */
  PersistedJobInfo(
      Optional<Long> id,
      Long createTime,
      ReplicationOperation operation,
      ReplicationStatus status,
      Optional<Path> srcPath,
      String srcClusterName,
      String srcDbName,
      String srcTableName,
      List<String> srcPartitionNames,
      Optional<String> srcObjectTldt,
      Optional<String> renameToDb,
      Optional<String> renameToTable,
      Optional<String> renameToPartition,
      Optional<Path> renameToPath,
      Map<String, String> extras) {
    if (id.isPresent()) {
      this.id = CompletableFuture.completedFuture(id.get());
      this.persistState = PersistState.PERSISTED;
    } else {
      this.id = new CompletableFuture<>();
      this.persistState = PersistState.PENDING;
    }
    this.createTime = createTime;
    this.operation = operation;
    this.status = status;
    this.srcPath = srcPath;
    this.srcClusterName = srcClusterName;
    this.srcDbName = srcDbName;
    this.srcTableName = srcTableName;
    if (srcPartitionNames != null) {
      this.srcPartitionNames = srcPartitionNames;
    } else {
      LOG.error("null srcPartitionNames passed in constructor", new Exception());
      this.srcPartitionNames = new ArrayList<>();
    }
    this.srcObjectTldt = srcObjectTldt;
    this.renameToDb = renameToDb;
    this.renameToTable = renameToTable;
    this.renameToPartition = renameToPartition;
    this.renameToPath = renameToPath;
    if (extras == null) {
      LOG.error("null extras passed in constructor", new Exception());
      this.extras = new HashMap<>();
    } else {
      this.extras = extras;
    }
  }

  void setPersisted(Long id) {
    if (this.persistState == PersistState.PERSISTED) {
      throw new RuntimeException("PersistedJobInfo.setPersisted can only be called once.");
    }
    this.persistState = PersistState.PERSISTED;
    this.id.complete(id);
  }

  public PersistState getPersistState() {
    return this.persistState;
  }

  /**
   * Returns the ID if it has been persisted. Should only be called if persisted.
   * @return the id
   */
  public Long getId() {
    try {
      if (this.id.isDone()) {
        return this.id.get();
      } else {
        throw new RuntimeException("getId should not be called before setPersisted().");
      }
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException("These exceptions should never be thrown.");
    }
  }

  public ReplicationOperation getOperation() {
    return operation;
  }

  public ReplicationStatus getStatus() {
    return status;
  }

  public String getSrcClusterName() {
    return srcClusterName;
  }

  public Optional<Path> getSrcPath() {
    return srcPath;
  }

  public String getSrcDbName() {
    return srcDbName;
  }

  public String getSrcTableName() {
    return srcTableName;
  }

  public List<String> getSrcPartitionNames() {
    return srcPartitionNames;
  }

  public Optional<String> getSrcObjectTldt() {
    return srcObjectTldt;
  }

  public void setStatus(ReplicationStatus status) {
    this.status = status;
  }

  public Optional<String> getRenameToDb() {
    return renameToDb;
  }

  public Optional<String> getRenameToTable() {
    return renameToTable;
  }

  public Optional<String> getRenameToPartition() {
    return renameToPartition;
  }

  public Optional<Path> getRenameToPath() {
    return renameToPath;
  }

  public Map<String, String> getExtras() {
    return extras;
  }

  public long getCreateTime() {
    return createTime;
  }

  @Override
  public String toString() {
    return "PersistedJobInfo{" + "id=" + id + ", operation=" + operation + ", createTime="
        + createTime + ", status=" + status + ", srcPath=" + srcPath + ", srcClusterName='"
        + srcClusterName + '\'' + ", srcDbName='" + srcDbName + '\'' + ", srcTableName='"
        + srcTableName + '\'' + ", srcPartitionNames=" + srcPartitionNames + ", srcObjectTldt='"
        + srcObjectTldt + '\'' + ", renameToDb='" + renameToDb + '\'' + ", renameToTable='"
        + renameToTable + '\'' + ", renameToPartition='" + renameToPartition + '\''
        + ", renameToPath=" + renameToPath + ", extras=" + extras + '}';
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }

    PersistedJobInfo that = (PersistedJobInfo) obj;

    if (createTime != that.createTime) {
      return false;
    }
    if (extras != null ? !extras.equals(that.extras) : that.extras != null) {
      return false;
    }
    // either the Future is determined and the value is equal, or they are the same future
    if (id != null ? !(id.equals(that.id)
        || (id.isDone() && id.getNow(null).equals(that.id.getNow(null)))) :
        that.id != null) {
      return false;
    }
    if (operation != that.operation) {
      return false;
    }
    if (renameToDb != null ? !renameToDb.equals(that.renameToDb) : that.renameToDb != null) {
      return false;
    }
    if (renameToPartition != null ? !renameToPartition.equals(that.renameToPartition)
        : that.renameToPartition != null) {
      return false;
    }
    if (renameToPath != null ? !renameToPath.equals(that.renameToPath)
                             : that.renameToPath != null) {
      return false;
    }
    if (renameToTable != null ? !renameToTable.equals(that.renameToTable)
        : that.renameToTable != null) {
      return false;
    }
    if (srcClusterName != null ? !srcClusterName.equals(that.srcClusterName)
        : that.srcClusterName != null) {
      return false;
    }
    if (srcDbName != null ? !srcDbName.equals(that.srcDbName) : that.srcDbName != null) {
      return false;
    }
    if (srcObjectTldt != null ? !srcObjectTldt.equals(that.srcObjectTldt)
        : that.srcObjectTldt != null) {
      return false;
    }
    if (srcPartitionNames != null ? !srcPartitionNames.equals(that.srcPartitionNames)
        : that.srcPartitionNames != null) {
      return false;
    }
    if (srcPath != null ? !srcPath.equals(that.srcPath) : that.srcPath != null) {
      return false;
    }
    if (srcTableName != null ? !srcTableName.equals(that.srcTableName)
                             : that.srcTableName != null) {
      return false;
    }
    if (status != that.status) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    int result = id != null ? id.hashCode() : 0;
    result = 31 * result + (int) (createTime ^ (createTime >>> 32));
    result = 31 * result + (operation != null ? operation.hashCode() : 0);
    result = 31 * result + (status != null ? status.hashCode() : 0);
    result = 31 * result + (srcPath != null ? srcPath.hashCode() : 0);
    result = 31 * result + (srcClusterName != null ? srcClusterName.hashCode() : 0);
    result = 31 * result + (srcDbName != null ? srcDbName.hashCode() : 0);
    result = 31 * result + (srcTableName != null ? srcTableName.hashCode() : 0);
    result = 31 * result + (srcPartitionNames != null ? srcPartitionNames.hashCode() : 0);
    result = 31 * result + (srcObjectTldt != null ? srcObjectTldt.hashCode() : 0);
    result = 31 * result + (renameToDb != null ? renameToDb.hashCode() : 0);
    result = 31 * result + (renameToTable != null ? renameToTable.hashCode() : 0);
    result = 31 * result + (renameToPartition != null ? renameToPartition.hashCode() : 0);
    result = 31 * result + (renameToPath != null ? renameToPath.hashCode() : 0);
    result = 31 * result + (extras != null ? extras.hashCode() : 0);
    return result;
  }

  /**
   * Creates a PersistedJobInfo with no ID.
   * @param operation operation
   * @param status status
   * @param srcPath srcPath
   * @param srcClusterName srcClusterName
   * @param srcTableSpec srcTableSpec
   * @param srcPartitionNames srcPartitionNames
   * @param srcTldt srcTldt
   * @param renameToObject renameToObject
   * @param renameToPath renameToPath
   * @param extras extras
   * @return An unpersisted PersistedJobInfo
   */
  public static PersistedJobInfo createDeferred(
      ReplicationOperation operation,
      ReplicationStatus status,
      Optional<Path> srcPath,
      String srcClusterName,
      HiveObjectSpec srcTableSpec,
      List<String> srcPartitionNames,
      Optional<String> srcTldt,
      Optional<HiveObjectSpec> renameToObject,
      Optional<Path> renameToPath,
      Map<String, String> extras) {
    long timestampMillisRounded = System.currentTimeMillis() / 1000L * 1000L;
    PersistedJobInfo persistedJobInfo =
        new PersistedJobInfo(Optional.empty(), timestampMillisRounded, operation, status, srcPath,
            srcClusterName, srcTableSpec.getDbName(), srcTableSpec.getTableName(),
            srcPartitionNames, srcTldt,
            renameToObject.map(HiveObjectSpec::getDbName),
            renameToObject.map(HiveObjectSpec::getTableName),
            renameToObject.map(HiveObjectSpec::getPartitionName),
            renameToPath, extras);
    return persistedJobInfo;
  }
}
