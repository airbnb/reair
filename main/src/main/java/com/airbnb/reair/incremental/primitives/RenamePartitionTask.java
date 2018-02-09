package com.airbnb.reair.incremental.primitives;

import com.airbnb.reair.common.DistCpException;
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
import com.airbnb.reair.multiprocessing.Lock;
import com.airbnb.reair.multiprocessing.LockSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Task that renames a partition. The expected modified time of the partition to rename on the
 * destination needs to be passed in to ensure that a newer partition with the same name is not
 * renamed.
 */
public class RenamePartitionTask implements ReplicationTask {

  private static final Log LOG = LogFactory.getLog(RenamePartitionTask.class);

  private Configuration conf;
  private DestinationObjectFactory destObjectFactory;
  private ObjectConflictHandler objectConflictHandler;
  private Cluster srcCluster;
  private Cluster destCluster;
  private HiveObjectSpec renameFromSpec;
  private HiveObjectSpec renameToSpec;
  private Optional<Path> renameFromPath;
  private Optional<Path> renameToPath;
  private Optional<String> renameFromPartitionTdlt;
  private DirectoryCopier directoryCopier;

  /**
   * Constructor for a task a that renames a partition.
   *
   * @param conf configuration object
   * @param destObjectFactory factory for creating objects for the destination cluster
   * @param objectConflictHandler handler for addressing conflicting tables/partitions on the
   *                              destination cluster
   * @param srcCluster source cluster
   * @param destCluster destination cluster
   * @param renameFromSpec specification for the Hive partition to rename from
   * @param renameToSpec specification for the Hive partition to rename to
   * @param renameFromPath the path for the partition to rename from
   * @param renameToPath the path to the partition to rename to
   * @param renameFromPartitionTdlt The expected modified time for the partitions. This should be
   *                                the transient_lastDdlTime value in the parameters field of the
   *                                Thrift object. If the time does not match, the task will not
   *                                rename the table.
   * @param directoryCopier runs directory copies through MR jobs
   */
  public RenamePartitionTask(
      Configuration conf,
      DestinationObjectFactory destObjectFactory,
      ObjectConflictHandler objectConflictHandler,
      Cluster srcCluster,
      Cluster destCluster,
      HiveObjectSpec renameFromSpec,
      HiveObjectSpec renameToSpec,
      Optional<Path> renameFromPath,
      Optional<Path> renameToPath,
      Optional<String> renameFromPartitionTdlt,
      DirectoryCopier directoryCopier) {
    this.conf = conf;
    this.destObjectFactory = destObjectFactory;
    this.objectConflictHandler = objectConflictHandler;
    this.srcCluster = srcCluster;
    this.destCluster = destCluster;
    this.renameFromSpec = renameFromSpec;
    this.renameToSpec = renameToSpec;
    this.renameFromPath = renameFromPath;
    this.renameToPath = renameToPath;
    this.renameFromPartitionTdlt = renameFromPartitionTdlt;
    this.directoryCopier = directoryCopier;
  }

  enum HandleRenameAction {
    RENAME_PARTITION, EXCHANGE_PARTITION, COPY_PARTITION, NO_OP
  }

  @Override
  public RunInfo runTask()
      throws ConfigurationException, HiveMetastoreException, DistCpException, IOException {
    LOG.debug("Renaming " + renameFromSpec + " to " + renameToSpec);

    if (!renameFromPartitionTdlt.isPresent()) {
      LOG.error("For safety, not completing rename since source " + " object TLDT is missing!");
      return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
    }
    String expectedTldt = renameFromPartitionTdlt.get();

    HiveMetastoreClient destMs = destCluster.getMetastoreClient();
    HiveMetastoreClient srcMs = srcCluster.getMetastoreClient();

    Partition freshSrcRenameToPart = srcMs.getPartition(renameToSpec.getDbName(),
        renameToSpec.getTableName(), renameToSpec.getPartitionName());

    Partition freshDestRenameToPart = destMs.getPartition(renameToSpec.getDbName(),
        renameToSpec.getTableName(), renameToSpec.getPartitionName());

    // Get a fresh copy of the metadata from the source Hive metastore
    Partition freshDestRenameFromPart = destMs.getPartition(renameFromSpec.getDbName(),
        renameFromSpec.getTableName(), renameFromSpec.getPartitionName());


    HandleRenameAction renameAction = null;
    // For cases where the table doesn't change, you can do a rename
    if (renameFromSpec.getDbName().equals(renameToSpec.getDbName())
        && renameFromSpec.getTableName().equals(renameToSpec.getTableName())) {
      renameAction = HandleRenameAction.RENAME_PARTITION;
    } else {
      // Otherwise, it needs to be an exchange.
      renameAction = HandleRenameAction.EXCHANGE_PARTITION;
    }

    // Check to see if transient_lastDdl times match between what was
    // renamed and what exists
    if (ReplicationUtils.transientLastDdlTimesMatch(freshSrcRenameToPart, freshDestRenameToPart)) {
      LOG.debug("Rename to partition exists on destination and has a "
          + "matching TLDT. Not doing anything");
      renameAction = HandleRenameAction.NO_OP;

    } else if (freshDestRenameToPart != null) {
      LOG.debug("Rename to partition already exists on destination, but "
          + "doesn't have a matching TLDT. Copying instead...");
      renameAction = HandleRenameAction.COPY_PARTITION;
    } else if (freshDestRenameFromPart == null) {
      LOG.warn(StringUtils.format(
          "Renamed-from partition %s " + "doesn't exist. " + "Copying %s to destination instead.",
          renameFromSpec, renameToSpec));
      renameAction = HandleRenameAction.COPY_PARTITION;
    } else if (!ReplicationUtils.transientLastDdlTimesMatch(expectedTldt,
        freshDestRenameFromPart)) {
      LOG.warn(StringUtils.format(
          "Destination partition %s doesn't " + "have the expected modified time", renameFromSpec));
      LOG.debug("Renamed from source table with a TLDT: " + renameFromPartitionTdlt);
      LOG.debug("Partition on destination: " + freshDestRenameFromPart);
      LOG.debug(String.format("Copying %s to destination instead", renameToSpec));
      renameAction = HandleRenameAction.COPY_PARTITION;
    } else {
      LOG.debug(String.format("Destination table (%s) matches " + "expected TLDT(%s) - will rename",
          renameFromSpec, ReplicationUtils.getTldt(freshDestRenameFromPart)));
      // Action set in the beginning
    }

    switch (renameAction) {
      case NO_OP:
        return new RunInfo(RunInfo.RunStatus.SUCCESSFUL, 0);

      case RENAME_PARTITION:
        LOG.debug(StringUtils.format("Renaming %s to %s", renameFromSpec, renameToSpec));
        Partition newPartitionOnDestination = new Partition(freshDestRenameFromPart);

        List<String> renameToPartitionValues =
            HiveUtils.partitionNameToValues(destMs, renameToSpec.getPartitionName());

        List<String> renameFromPartitionValues =
            HiveUtils.partitionNameToValues(srcMs, renameFromSpec.getPartitionName());

        newPartitionOnDestination.setValues(renameToPartitionValues);

        destMs.renamePartition(renameFromSpec.getDbName(), renameFromSpec.getTableName(),
            renameFromPartitionValues, newPartitionOnDestination);
        LOG.debug(StringUtils.format("Renamed %s to %s", renameFromSpec, renameToSpec));

        // After a rename, the partition should be re-copied to get the
        // correct modified time changes. With a proper rename, this
        // should be a mostly no-op.
        return copyPartition(renameToSpec, renameToPath);

      case EXCHANGE_PARTITION:
        // TODO: Exchange partition can't be done without HIVE-12865
        // Just do a copy instead.
        // fallthrough
      case COPY_PARTITION:
        return copyPartition(renameToSpec, renameToPath);

      default:
        throw new RuntimeException("Unhandled case: " + renameAction);
    }
  }

  private RunInfo copyPartition(HiveObjectSpec spec, Optional<Path> partitionLocation)
      throws ConfigurationException, HiveMetastoreException, DistCpException, IOException {
    CopyPartitionTask task = new CopyPartitionTask(conf, destObjectFactory, objectConflictHandler,
        srcCluster, destCluster, spec, partitionLocation, Optional.empty(), directoryCopier, true);
    return task.runTask();
  }

  @Override
  public LockSet getRequiredLocks() {
    LockSet lockSet = new LockSet();
    lockSet.add(new Lock(Lock.Type.EXCLUSIVE, renameFromSpec.toString()));
    lockSet.add(new Lock(Lock.Type.EXCLUSIVE, renameToSpec.toString()));
    return lockSet;
  }
}
