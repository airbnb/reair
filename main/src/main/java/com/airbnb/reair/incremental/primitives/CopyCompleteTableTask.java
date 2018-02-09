package com.airbnb.reair.incremental.primitives;

import com.airbnb.reair.common.DistCpException;
import com.airbnb.reair.common.HiveMetastoreClient;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.common.HiveUtils;
import com.airbnb.reair.incremental.DirectoryCopier;
import com.airbnb.reair.incremental.RunInfo;
import com.airbnb.reair.incremental.configuration.Cluster;
import com.airbnb.reair.incremental.configuration.ConfigurationException;
import com.airbnb.reair.incremental.configuration.DestinationObjectFactory;
import com.airbnb.reair.incremental.configuration.ObjectConflictHandler;
import com.airbnb.reair.multiprocessing.Lock;
import com.airbnb.reair.multiprocessing.LockSet;
import com.airbnb.reair.multiprocessing.ParallelJobExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Task that copies the entire table, include all the partitions if it's a partitioned table. To
 * reduce the number of distcp jobs necessary, this task tries to copy a common parent directory.
 * However, a better solution would be to use a copy tool that can copy multiple source and
 * destination directories simultaneously.
 */
public class CopyCompleteTableTask implements ReplicationTask {

  private static final Log LOG = LogFactory.getLog(CopyCompleteTableTask.class);

  private Configuration conf;
  private DestinationObjectFactory objectModifier;
  private ObjectConflictHandler objectConflictHandler;
  private Cluster srcCluster;
  private Cluster destCluster;
  private HiveObjectSpec spec;
  private Optional<Path> tableLocation;
  private ParallelJobExecutor copyPartitionsExecutor;
  private DirectoryCopier directoryCopier;

  /**
   * Constructs a task for copying an entire table.
   *
   * @param conf configuration object
   * @param objectFactory factory for creating objects for the destination cluster
   * @param objectConflictHandler handler for addressing conflicting tables/partitions on the
   *                              destination cluster
   * @param srcCluster source cluster
   * @param destCluster destination cluster
   * @param spec the Hive table specification
   * @param tableLocation the location of the table
   * @param copyPartitionsExecutor an executor for copying the partitions of a table
   * @param directoryCopier runs directory copies through MR jobs
   */
  public CopyCompleteTableTask(
      Configuration conf,
      DestinationObjectFactory objectFactory,
      ObjectConflictHandler objectConflictHandler,
      Cluster srcCluster,
      Cluster destCluster,
      HiveObjectSpec spec,
      Optional<Path> tableLocation,
      ParallelJobExecutor copyPartitionsExecutor,
      DirectoryCopier directoryCopier) {
    this.conf = conf;
    this.objectModifier = objectFactory;
    this.objectConflictHandler = objectConflictHandler;
    this.srcCluster = srcCluster;
    this.destCluster = destCluster;
    this.spec = spec;
    this.tableLocation = tableLocation;
    this.copyPartitionsExecutor = copyPartitionsExecutor;
    this.directoryCopier = directoryCopier;
  }

  @Override
  public RunInfo runTask()
      throws ConfigurationException, DistCpException, HiveMetastoreException, IOException {
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
      LOG.debug("Source table " + spec + " is a partitioned table");

      // Create a collection containing all the partitions that should
      // be copied.
      List<String> partitionNames = srcMs.getPartitionNames(spec.getDbName(), spec.getTableName());
      Map<HiveObjectSpec, Partition> specToPartition = new HashMap<>();
      for (String partitionName : partitionNames) {
        Partition partition =
            srcMs.getPartition(spec.getDbName(), spec.getTableName(), partitionName);

        if (partition == null) {
          throw new HiveMetastoreException(String.format("Partition %s does not exist!",
              spec));
        }

        HiveObjectSpec partitionSpec =
            new HiveObjectSpec(spec.getDbName(), spec.getTableName(), partitionName);
        specToPartition.put(partitionSpec, partition);
      }

      Optional<Path> commonDirectory = Optional.empty();

      if (specToPartition.size() > 0) {
        // If there are partitions to copy, see if there a common
        // parent directory to optimistically copy.
        Optional<Path> foundCommonDir =
            CopyPartitionsTask.findCommonDirectory(spec, specToPartition);
        if (foundCommonDir.isPresent()
            && objectModifier.shouldCopyData(foundCommonDir.get().toString())) {
          commonDirectory = foundCommonDir;
        } else {
          LOG.warn("Not copying common directory " + foundCommonDir);
        }
      }

      CopyPartitionsTask job = new CopyPartitionsTask(conf, objectModifier, objectConflictHandler,
          srcCluster, destCluster, spec, partitionNames, commonDirectory, copyPartitionsExecutor,
          directoryCopier);

      RunInfo copyPartitionsRunInfo = job.runTask();

      if (copyPartitionsRunInfo.getRunStatus() != RunInfo.RunStatus.SUCCESSFUL
          || copyPartitionsRunInfo.getRunStatus() != RunInfo.RunStatus.NOT_COMPLETABLE) {
        return copyPartitionsRunInfo;
      }

      CopyPartitionedTableTask copyTableTask = new CopyPartitionedTableTask(conf, objectModifier,
          objectConflictHandler, srcCluster, destCluster, spec, commonDirectory);
      RunInfo copyTableRunInfo = copyTableTask.runTask();

      return new RunInfo(copyTableRunInfo.getRunStatus(),
          copyPartitionsRunInfo.getBytesCopied() + copyTableRunInfo.getBytesCopied());
    } else {
      LOG.debug("Source table " + spec + " is an unpartitioned table");
      CopyUnpartitionedTableTask copyJob =
          new CopyUnpartitionedTableTask(conf, objectModifier, objectConflictHandler, srcCluster,
              destCluster, spec, tableLocation, directoryCopier, true);
      return copyJob.runTask();
    }
  }

  @Override
  public LockSet getRequiredLocks() {
    LockSet lockSet = new LockSet();
    lockSet.add(new Lock(Lock.Type.EXCLUSIVE, spec.toString()));
    return lockSet;
  }
}
