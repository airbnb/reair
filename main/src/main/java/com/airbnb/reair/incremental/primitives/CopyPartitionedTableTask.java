package com.airbnb.reair.incremental.primitives;

import com.airbnb.reair.common.HiveMetastoreClient;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.common.HiveUtils;
import com.airbnb.reair.incremental.ReplicationUtils;
import com.airbnb.reair.incremental.RunInfo;
import com.airbnb.reair.incremental.configuration.Cluster;
import com.airbnb.reair.incremental.configuration.DestinationObjectFactory;
import com.airbnb.reair.incremental.configuration.ObjectConflictHandler;
import com.airbnb.reair.incremental.deploy.ConfigurationKeys;
import com.airbnb.reair.multiprocessing.Lock;
import com.airbnb.reair.multiprocessing.LockSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.Optional;

/**
 * Task that copies metadata for a partitioned table.
 */
public class CopyPartitionedTableTask implements ReplicationTask {

  private static final Log LOG = LogFactory.getLog(CopyPartitionedTableTask.class);

  private Configuration conf;
  private DestinationObjectFactory objectModifier;
  private ObjectConflictHandler objectConflictHandler;
  private Cluster srcCluster;
  private Cluster destCluster;
  private HiveObjectSpec spec;
  private Optional<Path> srcPath;


  /**
   * Constructor for a task that copies the metadata for a partitioned table.
   * @param conf configuration object
   * @param destObjectFactory factory for creating objects for the destination cluster
   * @param objectConflictHandler handler for addressing conflicting tables/partitions on the
   *                              destination cluster
   * @param srcCluster source cluster
   * @param destCluster destination cluster
   * @param spec specification for the Hive partitioned table to copy
   * @param srcPath the path to the partition's data
   */
  public CopyPartitionedTableTask(
      Configuration conf,
      DestinationObjectFactory destObjectFactory,
      ObjectConflictHandler objectConflictHandler,
      Cluster srcCluster,
      Cluster destCluster,
      HiveObjectSpec spec,
      Optional<Path> srcPath) {
    this.conf = conf;
    this.objectModifier = destObjectFactory;
    this.objectConflictHandler = objectConflictHandler;
    this.srcCluster = srcCluster;
    this.destCluster = destCluster;
    this.spec = spec;
    this.srcPath = srcPath;
  }

  @Override
  public RunInfo runTask() throws HiveMetastoreException {
    LOG.debug("Copying " + spec);
    HiveMetastoreClient destMs = destCluster.getMetastoreClient();

    HiveMetastoreClient srcMs = srcCluster.getMetastoreClient();

    // Get a fresh copy of the metadata from the source Hive metastore
    Table freshSrcTable = srcMs.getTable(spec.getDbName(), spec.getTableName());

    if (freshSrcTable == null) {
      LOG.warn("Source table " + spec + " doesn't exist, so not " + "copying");
      return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
    }

    if (!HiveUtils.isPartitioned(freshSrcTable)) {
      LOG.warn("Not copying " + spec + " since it's not partitioned");
      return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
    }

    // Check the table that exists already in the destination cluster
    Table existingTable = destMs.getTable(spec.getDbName(), spec.getTableName());

    Table destTable =
        objectModifier.createDestTable(srcCluster, destCluster, freshSrcTable, existingTable);


    if (existingTable != null) {
      LOG.debug("Table " + spec + " exists on destination!");

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

    // Refresh in case the conflict handler did something
    existingTable = destMs.getTable(spec.getDbName(), spec.getTableName());

    // Figure out what to do with the table
    MetadataAction action = MetadataAction.NOOP;
    if (existingTable == null) {
      action = MetadataAction.CREATE;
    } else if (!ReplicationUtils.stripNonComparables(existingTable)
        .equals(ReplicationUtils.stripNonComparables(destTable))) {
      action = MetadataAction.ALTER;
    }

    // Take necessary action
    switch (action) {
      case CREATE:
        LOG.debug("Creating " + spec + " since it does not exist on " + "the destination");
        ReplicationUtils.createDbIfNecessary(srcMs, destMs, destTable.getDbName());
        LOG.debug("Creating: " + destTable);
        destMs.createTable(destTable);
        LOG.debug("Successfully created table " + spec);
        break;

      case ALTER:
        LOG.debug("Altering table " + spec + " on destination");
        LOG.debug("Existing table: " + existingTable);
        LOG.debug("Replacement table: " + destTable);
        destMs.alterTable(destTable.getDbName(), destTable.getTableName(), destTable);
        LOG.debug("Successfully altered " + spec);
        break;

      case NOOP:
        LOG.debug("Destination table " + spec + " is up to date, so " + "not doing anything");
        break;

      default:
        throw new RuntimeException("Unhandled case!");
    }

    return new RunInfo(RunInfo.RunStatus.SUCCESSFUL, 0);
  }

  @Override
  public LockSet getRequiredLocks() {
    LockSet lockSet = new LockSet();
    lockSet.add(new Lock(Lock.Type.EXCLUSIVE, spec.toString()));
    return lockSet;
  }
}
