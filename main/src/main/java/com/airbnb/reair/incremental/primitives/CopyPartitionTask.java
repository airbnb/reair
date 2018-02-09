package com.airbnb.reair.incremental.primitives;

import com.airbnb.reair.common.DistCpException;
import com.airbnb.reair.common.FsUtils;
import com.airbnb.reair.common.HiveMetastoreClient;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.io.IOException;
import java.util.Arrays;
import java.util.Optional;

/**
 * Task that copies a single Hive partition - both data and metadata.
 *
 * <p>Known issue: if multiple copy partition jobs are kicked off, and the partitioned table doesn't
 * exist on the destination, then it's possible that multiple copy partition jobs try to create the
 * same partitioned table.
 *
 * <p>This can result in a failure, but should be corrected on a retry.
 */

public class CopyPartitionTask implements ReplicationTask {

  private static final Log LOG = LogFactory.getLog(CopyPartitionTask.class);

  private Configuration conf;
  private DestinationObjectFactory destObjectFactory;
  private ObjectConflictHandler objectConflictHandler;
  private Cluster srcCluster;
  private Cluster destCluster;
  private HiveObjectSpec spec;
  private Optional<Path> partitionLocation;
  private Optional<Path> optimisticCopyRoot;
  private DirectoryCopier directoryCopier;
  private boolean allowDataCopy;

  /**
   * Constructor for a task that copies a single Hive partition.
   *
   * @param conf configuration object
   * @param destObjectFactory factory for creating objects for the destination cluster
   * @param objectConflictHandler handler for addressing conflicting tables/partitions on the
   *                              destination cluster
   * @param srcCluster source cluster
   * @param destCluster destination cluster
   * @param spec specification for the Hive partition to copy
   * @param partitionLocation the location for the partition, if applicable
   * @param optimisticCopyRoot if data for this partitioned was copied in advance, the root
   *                           directory where the data was copied to. For example, if the partition
   *                           data was located in /a/b/c and /a was copied to /tmp/copy/a, then the
   *                           copy root directory is /tmp/copy
   * @param directoryCopier runs directory copies through MR jobs
   * @param allowDataCopy Whether to copy data for this partition. If set to false, the task will
   *                      check to see if the data exists already and if not, it will fail the task.
   */
  public CopyPartitionTask(
      Configuration conf,
      DestinationObjectFactory destObjectFactory,
      ObjectConflictHandler objectConflictHandler,
      Cluster srcCluster,
      Cluster destCluster,
      HiveObjectSpec spec,
      Optional<Path> partitionLocation,
      Optional<Path> optimisticCopyRoot,
      DirectoryCopier directoryCopier,
      boolean allowDataCopy) {
    this.conf = conf;
    this.destObjectFactory = destObjectFactory;
    this.objectConflictHandler = objectConflictHandler;
    this.srcCluster = srcCluster;
    this.destCluster = destCluster;
    this.spec = spec;
    this.partitionLocation = partitionLocation;
    this.optimisticCopyRoot = optimisticCopyRoot;
    this.directoryCopier = directoryCopier;
    this.allowDataCopy = allowDataCopy;
  }

  @Override
  public RunInfo runTask()
      throws ConfigurationException, HiveMetastoreException, DistCpException, IOException {
    LOG.debug("Copying " + spec);

    HiveMetastoreClient destMs = destCluster.getMetastoreClient();
    HiveMetastoreClient srcMs = srcCluster.getMetastoreClient();

    Partition freshSrcPartition =
        srcMs.getPartition(spec.getDbName(), spec.getTableName(), spec.getPartitionName());

    if (freshSrcPartition == null) {
      LOG.warn("Source partition " + spec + " does not exist, so not " + "copying");
      return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
    }

    if (!conf.getBoolean(ConfigurationKeys.BATCH_JOB_OVERWRITE_NEWER, true)) {
      Partition freshDestPartition =
          destMs.getPartition(spec.getDbName(), spec.getTableName(), spec.getPartitionName());
      if (ReplicationUtils.isSrcOlder(freshSrcPartition, freshDestPartition)) {
        LOG.warn(String.format(
            "Source %s (%s) is older than destination (%s), so not copying",
            spec,
            ReplicationUtils.getLastModifiedTime(freshSrcPartition),
            ReplicationUtils.getLastModifiedTime(freshDestPartition)));
        return new RunInfo(RunInfo.RunStatus.DEST_IS_NEWER, 0);
      }
    }

    // Before copying a partition, first make sure that table is up to date
    Table srcTable = srcMs.getTable(spec.getDbName(), spec.getTableName());
    Table destTable = destMs.getTable(spec.getDbName(), spec.getTableName());

    if (srcTable == null) {
      LOG.warn("Source table " + spec + " doesn't exist, so not " + "copying");
      return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
    }

    if (destTable == null || !ReplicationUtils.schemasMatch(srcTable, destTable)) {
      LOG.warn("Copying source table over to the destination since "
          + "schemas do not match. (source: " + srcTable + " destination: " + destTable + ")");
      CopyPartitionedTableTask copyTableJob =
          new CopyPartitionedTableTask(conf, destObjectFactory, objectConflictHandler, srcCluster,
              destCluster, spec.getTableSpec(), ReplicationUtils.getLocation(srcTable));
      RunInfo status = copyTableJob.runTask();
      if (status.getRunStatus() != RunInfo.RunStatus.SUCCESSFUL) {
        LOG.error("Failed to copy " + spec.getTableSpec());
        return new RunInfo(RunInfo.RunStatus.FAILED, 0);
      }
    }

    Partition existingPartition =
        destMs.getPartition(spec.getDbName(), spec.getTableName(), spec.getPartitionName());

    Partition destPartition = destObjectFactory.createDestPartition(srcCluster, destCluster,
        freshSrcPartition, existingPartition);

    if (existingPartition != null) {
      LOG.debug("Partition " + spec + " already exists!");
      objectConflictHandler.handleCopyConflict(srcCluster, destCluster, freshSrcPartition,
          existingPartition);
    }

    // Copy HDFS data
    long bytesCopied = 0;

    Optional<Path> srcPath = ReplicationUtils.getLocation(freshSrcPartition);
    Optional<Path> destPath = ReplicationUtils.getLocation(destPartition);

    // Try to copy data only if the location is defined and the location
    // for the destination object is different. Usually, the location will
    // be different as it will be situated on a different HDFS, but for
    // S3 backed tables, the location may not change.
    boolean needToCopy = false;
    // TODO: An optimization can be made here to check for directories that
    // already match and no longer need to be copied.
    if (srcPath.isPresent() && !srcPath.equals(destPath)) {
      // If a directory was copied optimistically, check if the data is
      // there. If the data is there and it matches up with what is
      // expected, then the directory can be moved into place.
      if (optimisticCopyRoot.isPresent()) {
        Path srcLocation = new Path(freshSrcPartition.getSd().getLocation());

        // Assume that on the source, a table is stored at /a, and the
        // partitions are stored at /a/ds=1 and /a/ds=1.
        //
        // For this optimization, the source directory (/u/a) was
        // copied to a temporary location (/tmp/u/a). The optimistic
        // copy root would be /tmp. To figure out the directory
        // containing a partition's data, start with the optimistic
        // copy root and add the relative path from / - e.g.
        // /tmp + u/a/ds=1 = /tmp/u/a/ds=1
        Path copiedPartitionDataLocation = new Path(optimisticCopyRoot.get(),
            StringUtils.stripStart(srcLocation.toUri().getPath(), "/"));

        if (directoryCopier.equalDirs(srcLocation, copiedPartitionDataLocation)) {
          // In this case, the data is there and we can move the
          // directory to the expected location.
          Path destinationPath = new Path(destPartition.getSd().getLocation());

          FsUtils.replaceDirectory(conf, copiedPartitionDataLocation, destinationPath);
        } else {
          needToCopy = !directoryCopier.equalDirs(srcPath.get(), destPath.get());
        }
      } else {
        needToCopy = !directoryCopier.equalDirs(srcPath.get(), destPath.get());;
      }
    }

    if (srcPath.isPresent() && destPath.isPresent() && needToCopy) {
      if (!allowDataCopy) {
        LOG.debug(String.format("Need to copy %s to %s, but data " + "copy is not allowed", srcPath,
            destPath));
        return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
      }

      if (!FsUtils.dirExists(conf, srcPath.get())) {
        LOG.error("Source path " + srcPath + " does not exist!");
        return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
      }

      bytesCopied = directoryCopier.copy(srcPath.get(), destPath.get(),
          Arrays.asList(srcCluster.getName(), spec.getDbName(), spec.getTableName()));
    }

    // Figure out what to do with the table
    MetadataAction action = MetadataAction.NOOP;
    if (existingPartition == null) {
      // If the partition doesn't exist on the destination, we need to
      // create it.
      action = MetadataAction.CREATE;
    } else if (!ReplicationUtils.stripNonComparables(existingPartition)
        .equals(ReplicationUtils.stripNonComparables(destPartition))) {
      // The partition exists on the destination, but some of the metadata
      // attributes are not as expected. This can be fixed with an alter
      // call.
      action = MetadataAction.ALTER;
    }

    // Take necessary action
    switch (action) {

      case CREATE:
        ReplicationUtils.createDbIfNecessary(srcMs, destMs, destPartition.getDbName());

        LOG.debug("Creating " + spec + " since it does not exist on " + "the destination");
        destMs.addPartition(destPartition);
        LOG.debug("Successfully created " + spec);
        break;

      case ALTER:
        LOG.debug("Altering partition " + spec + " on destination");
        destMs.alterPartition(destPartition.getDbName(), destPartition.getTableName(),
            destPartition);
        break;

      case NOOP:
        LOG.debug("Not doing anything for " + spec);
        break;

      default:
        throw new RuntimeException("Unhandled case!");
    }

    return new RunInfo(RunInfo.RunStatus.SUCCESSFUL, bytesCopied);
  }

  public HiveObjectSpec getSpec() {
    return this.spec;
  }

  @Override
  public LockSet getRequiredLocks() {
    LockSet lockSet = new LockSet();
    lockSet.add(new Lock(Lock.Type.SHARED, spec.getTableSpec().toString()));
    lockSet.add(new Lock(Lock.Type.EXCLUSIVE, spec.toString()));
    return lockSet;
  }
}
