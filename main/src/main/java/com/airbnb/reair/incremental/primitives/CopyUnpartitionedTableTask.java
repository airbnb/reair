package com.airbnb.reair.incremental.primitives;

import com.airbnb.reair.common.DistCpException;
import com.airbnb.reair.common.FsUtils;
import com.airbnb.reair.common.HiveMetastoreClient;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.common.HiveUtils;
import com.airbnb.reair.incremental.DirectoryCopier;
import com.airbnb.reair.incremental.ReplicationUtils;
import com.airbnb.reair.incremental.RunInfo;
import com.airbnb.reair.incremental.configuration.Cluster;
import com.airbnb.reair.incremental.configuration.ConfigurationException;
import com.airbnb.reair.incremental.configuration.DestinationObjectFactory;
import com.airbnb.reair.incremental.configuration.ObjectConflictHandler;
import com.airbnb.reair.incremental.deploy.ConfigurationKeys;
import com.airbnb.reair.multiprocessing.Lock;
import com.airbnb.reair.multiprocessing.LockSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Table;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

/**
 * Task that copies an unpartitioned table, copying data if allowed and necessary.
 */
public class CopyUnpartitionedTableTask implements ReplicationTask {
  private static final Log LOG = LogFactory.getLog(CopyUnpartitionedTableTask.class);

  private Configuration conf;
  private DestinationObjectFactory objectModifier;
  private ObjectConflictHandler objectConflictHandler;
  private Cluster srcCluster;
  private Cluster destCluster;
  private HiveObjectSpec spec;
  private Optional<Path> tableLocation;
  private DirectoryCopier directoryCopier;
  private boolean allowDataCopy;

  /**
   * Constructor for a task that copies an unpartitioned table.
   *
   * @param conf configuration object
   * @param destObjectFactory factory for creating objects for the destination cluster
   * @param objectConflictHandler handler for addressing conflicting tables/partitions on the
   *                              destination cluster
   * @param srcCluster source cluster
   * @param destCluster destination cluster
   * @param spec specification for the Hive partitioned table to copy
   * @param tableLocation the location of the table
   * @param directoryCopier runs directory copies through MR jobs
   * @param allowDataCopy Whether to copy data for this partition. If set to false, the task will
   *                      check to see if the data exists already and if not, it will fail the task.
   */
  public CopyUnpartitionedTableTask(
      Configuration conf,
      DestinationObjectFactory destObjectFactory,
      ObjectConflictHandler objectConflictHandler,
      Cluster srcCluster,
      Cluster destCluster,
      HiveObjectSpec spec,
      Optional<Path> tableLocation,
      DirectoryCopier directoryCopier,
      boolean allowDataCopy) {
    this.conf = conf;
    this.objectModifier = destObjectFactory;
    this.objectConflictHandler = objectConflictHandler;
    this.srcCluster = srcCluster;
    this.destCluster = destCluster;
    this.tableLocation = tableLocation;
    this.spec = spec;
    this.directoryCopier = directoryCopier;
    this.allowDataCopy = allowDataCopy;
  }

  @Override
  public RunInfo runTask()
      throws ConfigurationException, HiveMetastoreException, DistCpException, IOException {
    LOG.debug("Copying " + spec);

    HiveMetastoreClient destMs = destCluster.getMetastoreClient();
    HiveMetastoreClient srcMs = srcCluster.getMetastoreClient();

    // Get a fresh copy of the metadata from the source Hive metastore
    Table freshSrcTable = srcMs.getTable(spec.getDbName(), spec.getTableName());

    if (freshSrcTable == null) {
      LOG.warn("Source table " + spec + " doesn't exist, so not " + "copying");
      return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
    }

    if (HiveUtils.isPartitioned(freshSrcTable)) {
      LOG.warn("Source table " + spec + " is a partitioned table, so " + "not copying");
      return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
    }

    // Check the table that exists already in the destination cluster
    Table existingTable = destMs.getTable(spec.getDbName(), spec.getTableName());

    if (existingTable != null) {
      LOG.debug("Table " + spec + " exists on destination");

      if (!conf.getBoolean(ConfigurationKeys.BATCH_JOB_OVERWRITE_NEWER, true)) {
        Table freshDestTable = existingTable;
        if (ReplicationUtils.isSrcOlder(freshSrcTable, freshDestTable)) {
          LOG.warn(String.format(
              "Source %s (%s) is older than destination (%s), so not copying",
              spec,
              ReplicationUtils.getLastModifiedTime(freshSrcTable),
              ReplicationUtils.getLastModifiedTime(freshDestTable)));
          return new RunInfo(RunInfo.RunStatus.DEST_IS_NEWER, 0);
        }
      }

      objectConflictHandler.handleCopyConflict(srcCluster, destCluster, freshSrcTable,
          existingTable);
    }

    Table destTable =
        objectModifier.createDestTable(srcCluster, destCluster, freshSrcTable, existingTable);

    // Refresh in case the conflict handler did something
    existingTable = destMs.getTable(spec.getDbName(), spec.getTableName());

    // Copy HDFS data if the location has changed in the destination object.
    // Usually, this is the case, but for S3 backed tables, the location
    // doesn't change.

    Optional<Path> srcPath = ReplicationUtils.getLocation(freshSrcTable);
    Optional<Path> destPath = ReplicationUtils.getLocation(destTable);

    boolean needToCopy = srcPath.isPresent() && !srcPath.equals(destPath)
        && !directoryCopier.equalDirs(srcPath.get(), destPath.get());

    long bytesCopied = 0;

    if (needToCopy) {
      if (!allowDataCopy) {
        LOG.debug(String.format("Need to copy %s to %s, but data "
            + "copy is not allowed", srcPath,
            destPath));
        return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
      }

      if (!FsUtils.dirExists(conf, srcPath.get())) {
        LOG.debug(String.format("Need to copy %s to %s, but "
            + "source directory is missing",
            srcPath, destPath));
        return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
      }

      // Copy directory
      bytesCopied = directoryCopier.copy(srcPath.get(), destPath.get(),
          Arrays.asList(srcCluster.getName(), spec.getDbName(), spec.getTableName()));
    } else {
      LOG.debug("Not copying data");
    }

    // Figure out what to do with the table
    MetadataAction action = MetadataAction.NOOP;
    if (existingTable == null) {
      action = MetadataAction.CREATE;
    } else if (!ReplicationUtils.stripNonComparables(existingTable)
        .equals(ReplicationUtils.stripNonComparables(destTable))) {
      action = MetadataAction.ALTER;
    }

    switch (action) {

      case CREATE:
        LOG.debug("Creating " + spec + " since it does not exist on " + "the destination");
        ReplicationUtils.createDbIfNecessary(srcMs, destMs, destTable.getDbName());
        LOG.debug("Creating: " + destTable);
        destMs.createTable(destTable);
        LOG.debug("Successfully created " + spec);
        break;

      case ALTER:
        LOG.debug("Altering table " + spec + " on destination");
        LOG.debug("Existing table: " + existingTable);
        LOG.debug("New table: " + destTable);
        destMs.alterTable(destTable.getDbName(), destTable.getTableName(), destTable);
        LOG.debug("Successfully altered " + spec);
        break;

      case NOOP:
        LOG.debug("Destination table is up to date - not doing " + "anything for " + spec);
        break;

      default:
        throw new RuntimeException("Unhandled case: " + action);
    }

    return new RunInfo(RunInfo.RunStatus.SUCCESSFUL, bytesCopied);
  }

  @Override
  public LockSet getRequiredLocks() {
    LockSet lockSet = new LockSet();
    lockSet.add(new Lock(Lock.Type.EXCLUSIVE, spec.toString()));
    return lockSet;
  }
}
