package com.airbnb.reair.incremental.primitives;

import com.airbnb.reair.common.DistCpException;
import com.airbnb.reair.common.FsUtils;
import com.airbnb.reair.common.HiveMetastoreClient;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.common.HiveUtils;
import com.airbnb.reair.common.PathBuilder;
import com.airbnb.reair.incremental.DirectoryCopier;
import com.airbnb.reair.incremental.ReplicationUtils;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;

/**
 * Task that copies multiple partitions. To reduce the number of distcp jobs necessary, this task
 * tries to copy a common parent directory of those partitions. However, a better solution would be
 * to use a copy tool that can copy multiple source and destination directories simultaneously.
 */
public class CopyPartitionsTask implements ReplicationTask {

  private static final Log LOG = LogFactory.getLog(CopyPartitionsTask.class);

  private Configuration conf;
  private DestinationObjectFactory objectModifier;
  private ObjectConflictHandler objectConflictHandler;
  private Cluster srcCluster;
  private Cluster destCluster;
  private HiveObjectSpec srcTableSpec;
  private List<String> partitionNames;
  private Optional<Path> commonDirectory;
  private ParallelJobExecutor copyPartitionsExecutor;
  private DirectoryCopier directoryCopier;


  /**
   * Constructor for a task to copy multiple partitions.
   *
   * @param conf configuration object
   * @param destObjectFactory factory for creating objects for the destination cluster
   * @param objectConflictHandler handler for addressing conflicting tables/partitions on the
   *                              destination cluster
   * @param srcCluster source cluster
   * @param destCluster destination cluster
   * @param srcTableSpec Hive specification for the table that these partitions belong to
   * @param partitionNames names of the partitions to copy
   * @param commonDirectory the common ancestor directory for the partitions, if applicable
   * @param copyPartitionsExecutor an executor for copying the partitions of a table
   * @param directoryCopier runs directory copies through MR jobs
   */
  public CopyPartitionsTask(
      Configuration conf,
      DestinationObjectFactory destObjectFactory,
      ObjectConflictHandler objectConflictHandler,
      Cluster srcCluster,
      Cluster destCluster,
      HiveObjectSpec srcTableSpec,
      List<String> partitionNames,
      Optional<Path> commonDirectory,
      ParallelJobExecutor copyPartitionsExecutor,
      DirectoryCopier directoryCopier) {
    this.conf = conf;
    this.objectModifier = destObjectFactory;
    this.objectConflictHandler = objectConflictHandler;
    this.srcCluster = srcCluster;
    this.destCluster = destCluster;
    this.srcTableSpec = srcTableSpec;
    this.partitionNames = partitionNames;
    this.commonDirectory = commonDirectory;
    this.copyPartitionsExecutor = copyPartitionsExecutor;
    this.directoryCopier = directoryCopier;
  }

  /**
   * Find the common directory for a set of partitions if one exists. For example if the partition
   * ds=1 has a location /a/b/ds=1 and the partition ds=2 has a location /a/b/ds=2, then the common
   * directory is /a/b
   *
   * @param srcTableSpec specification for the Hive table that these partitions belong to
   * @param specToPartition a map from the Hive partition specification to the partition object
   * @return the common directory for the partition, if one exists
   */
  public static Optional<Path> findCommonDirectory(
      HiveObjectSpec srcTableSpec,
      Map<HiveObjectSpec, Partition> specToPartition) {
    // Sanity check - verify that all the specified objects are partitions
    // and that they are from the same table

    for (HiveObjectSpec spec : specToPartition.keySet()) {

      if (!srcTableSpec.equals(spec.getTableSpec())) {
        throw new RuntimeException(
            "Spec " + spec + " does not " + "match the source table spec " + srcTableSpec);
      }

      if (!spec.isPartition()) {
        throw new RuntimeException("Partition not specified: " + spec);
      }
    }

    // Collect all the partition locations
    Set<Path> partitionLocations = new HashSet<>();
    for (Map.Entry<HiveObjectSpec, Partition> entry : specToPartition.entrySet()) {
      partitionLocations.add(new Path(entry.getValue().getSd().getLocation()));
    }
    // Find the common subdirectory among all the partitions
    // TODO: This may copy more data than necessary - use multi directory
    // copy instead once it's available.
    Optional<Path> commonDirectory = ReplicationUtils.getCommonDirectory(partitionLocations);
    LOG.debug("Common directory of partitions is " + commonDirectory);
    return commonDirectory;
  }

  @Override
  public RunInfo runTask()
      throws HiveMetastoreException, DistCpException, IOException,
      HiveMetastoreException, ConfigurationException {
    LOG.debug("Copying partitions from " + srcTableSpec);
    HiveMetastoreClient destMs = destCluster.getMetastoreClient();
    HiveMetastoreClient srcMs = srcCluster.getMetastoreClient();


    // Get a fresh copy of the metadata from the source Hive metastore
    Table freshSrcTable = srcMs.getTable(srcTableSpec.getDbName(), srcTableSpec.getTableName());

    if (freshSrcTable == null) {
      LOG.warn("Source table " + srcTableSpec + " doesn't exist, so not " + "copying");
      return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
    }

    if (!HiveUtils.isPartitioned(freshSrcTable)) {
      LOG.warn(
          "Source table " + srcTableSpec + " is not a a partitioned table," + " so not copying");
      return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
    }

    Optional<Path> tableLocation = ReplicationUtils.getLocation(freshSrcTable);
    LOG.debug("Location of table " + srcTableSpec + " is " + tableLocation);

    // If possible, copy the common directory in a single distcp job.
    // We call this the optimistic copy as this should result in no
    // additional distcp jobs when copying the partitions.

    long bytesCopied = 0;
    boolean doOptimisticCopy = false;

    if (commonDirectory.isPresent() && tableLocation.isPresent()
        && (tableLocation.equals(commonDirectory)
            || FsUtils.isSubDirectory(tableLocation.get(), commonDirectory.get()))) {
      Path commonDir = commonDirectory.get();
      // Get the size of all the partitions in the common directory and
      // check if the size of the common directory is approximately
      // the same size

      long sizeOfPartitionsInCommonDirectory = 0;
      for (String partitionName : partitionNames) {
        Partition partition = srcMs.getPartition(
            srcTableSpec.getDbName(),
            srcTableSpec.getTableName(),
            partitionName);
        if (partition != null && partition.getSd().getLocation() != null) {
          Path partitionLocation = new Path(partition.getSd().getLocation());
          if (FsUtils.isSubDirectory(commonDir, partitionLocation)
              && FsUtils.dirExists(conf, partitionLocation)) {
            sizeOfPartitionsInCommonDirectory +=
                FsUtils.getSize(conf, partitionLocation, Optional.empty());
          }
        }
      }

      if (!FsUtils.dirExists(conf, commonDir)) {
        LOG.debug(String.format("Common dir: %s does not exist", commonDir));
      } else if (!FsUtils.exceedsSize(conf, commonDir, sizeOfPartitionsInCommonDirectory * 2)) {
        doOptimisticCopy = true;
      } else {
        LOG.debug(String.format(
            "Size of common directory %s is much " + "bigger than the size of the partitions in "
                + "the common directory (%s). Hence, not " + "copying the common directory",
            commonDir, sizeOfPartitionsInCommonDirectory));
      }
    }

    Optional<Path> optimisticCopyDir = Optional.empty();
    // isPresent() isn't necessary, as doOptimisticCopy implies it's set.
    if (commonDirectory.isPresent() && doOptimisticCopy) {
      Path commonDir = commonDirectory.get();
      // Check if the common directory is the same on the destination
      String destinationLocation =
          objectModifier.modifyLocation(srcCluster, destCluster, commonDir.toString());
      Path destinationLocationPath = new Path(destinationLocation);

      if (!objectModifier.shouldCopyData(destinationLocation)) {
        LOG.debug("Skipping copy of destination location " + commonDirectory
            + " due to destination " + "object factory");
      } else if (!FsUtils.dirExists(conf, commonDir)) {
        LOG.debug("Skipping copy of destination location " + commonDirectory
            + " since it does not exist");
      } else if (FsUtils.equalDirs(conf, commonDir, destinationLocationPath)) {
        LOG.debug("Skipping copying common directory " + commonDir + " since it matches "
            + destinationLocationPath);
      } else {
        LOG.debug("Optimistically copying common directory " + commonDir);
        Random random = new Random();
        long randomLong = random.nextLong();

        Path path =
            new PathBuilder(destCluster.getTmpDir()).add("distcp_tmp").add(srcCluster.getName())
                .add("optimistic_copy").add(Long.toString(randomLong)).toPath();
        optimisticCopyDir = Optional.of(path);

        bytesCopied += copyWithStructure(commonDir, path);
      }
    }

    // Now copy all the partitions
    CopyPartitionsCounter copyPartitionsCounter = new CopyPartitionsCounter();
    long expectedCopyCount = 0;

    for (String partitionName : partitionNames) {
      Partition srcPartition =
          srcMs.getPartition(srcTableSpec.getDbName(), srcTableSpec.getTableName(), partitionName);
      HiveObjectSpec partitionSpec =
          new HiveObjectSpec(srcTableSpec.getDbName(), srcTableSpec.getTableName(), partitionName);

      if (srcPartition == null) {
        LOG.warn("Not copying missing partition: " + partitionSpec);
        continue;
      }

      CopyPartitionTask copyPartitionTask = new CopyPartitionTask(conf, objectModifier,
          objectConflictHandler, srcCluster, destCluster, partitionSpec,
          ReplicationUtils.getLocation(srcPartition), optimisticCopyDir, directoryCopier, true);

      CopyPartitionJob copyPartitionJob =
          new CopyPartitionJob(copyPartitionTask, copyPartitionsCounter);

      copyPartitionsExecutor.add(copyPartitionJob);
      expectedCopyCount++;
    }

    while (true) {
      LOG.debug(String.format("Copied %s out of %s partitions",
          copyPartitionsCounter.getCompletionCount(), expectedCopyCount));

      if (copyPartitionsCounter.getCompletionCount() == expectedCopyCount) {
        break;
      }

      try {
        Thread.sleep(5 * 1000);
      } catch (InterruptedException e) {
        LOG.error("Got interrupted!");
        throw new RuntimeException(e);
      }
    }

    bytesCopied += copyPartitionsCounter.getBytesCopied();

    return new RunInfo(RunInfo.RunStatus.SUCCESSFUL, bytesCopied);
  }

  /**
   * Copies the source directory to the destination directory while preserving structure. i.e. if
   * copying /a/b/c to the destination directory /d, then /d/a/b/c will be created and contain files
   * from /a/b/c.
   *
   * @param srcDir source directory
   * @param destDir destination directory
   * @return total number of bytes copied
   * @throws IOException if there is an error accessing the filesystem
   * @throws DistCpException if there is an error copying the data
   * @throws ConfigurationException if the config is improper
   */
  private long copyWithStructure(Path srcDir, Path destDir)
      throws ConfigurationException, DistCpException, IOException {

    PathBuilder dirBuilder = new PathBuilder(destDir);
    // Preserve the directory structure within the dest directory
    // Decompose a directory like /a/b/c and add a, b, c as subdirectories
    // within the tmp direcotry
    List<String> pathElements =
        new ArrayList<>(Arrays.asList(srcDir.toUri().getPath().split("/")));
    // When splitting a path like '/a/b/c', the first element is ''
    if (pathElements.get(0).equals("")) {
      pathElements.remove(0);
    }
    for (String pathElement : pathElements) {
      dirBuilder.add(pathElement);
    }
    Path destPath = dirBuilder.toPath();

    // Copy directory
    long bytesCopied = directoryCopier.copy(srcDir, destPath,
        Arrays.asList(srcCluster.getName(), "copy_with_structure"));

    return bytesCopied;
  }

  @Override
  public LockSet getRequiredLocks() {
    LockSet lockSet = new LockSet();
    lockSet.add(new Lock(Lock.Type.SHARED, srcTableSpec.toString()));

    for (String partitionName : partitionNames) {
      HiveObjectSpec partitionSpec =
          new HiveObjectSpec(srcTableSpec.getDbName(), srcTableSpec.getTableName(), partitionName);
      lockSet.add(new Lock(Lock.Type.EXCLUSIVE, partitionSpec.toString()));
    }
    return lockSet;
  }

}
