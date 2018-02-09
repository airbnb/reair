package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.airbnb.reair.common.DistCpException;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.incremental.ReplicationUtils;
import com.airbnb.reair.incremental.RunInfo;
import com.airbnb.reair.incremental.configuration.ConfigurationException;
import com.airbnb.reair.incremental.primitives.CopyPartitionsTask;
import com.airbnb.reair.multiprocessing.ParallelJobExecutor;
import com.airbnb.reair.utils.ReplicationTestUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CopyPartitionsTaskTest extends MockClusterTest {

  private static ParallelJobExecutor jobExecutor = new ParallelJobExecutor(1);

  @BeforeClass
  public static void setupClass() throws IOException, SQLException {
    MockClusterTest.setupClass();
    jobExecutor.start();
  }

  @Test
  public void testCopyPartitions()
      throws ConfigurationException, IOException, HiveMetastoreException, DistCpException {
    // Create a partitioned table in the source
    HiveObjectSpec tableSpec = new HiveObjectSpec("test_db", "test_table");
    ReplicationTestUtils.createPartitionedTable(conf, srcMetastore, tableSpec,
        TableType.MANAGED_TABLE, srcWarehouseRoot);

    // Create several partitions in the source table
    HiveObjectSpec partitionSpec1 = new HiveObjectSpec("test_db", "test_table", "ds=1/hr=1");
    HiveObjectSpec partitionSpec2 = new HiveObjectSpec("test_db", "test_table", "ds=1/hr=2");
    HiveObjectSpec partitionSpec3 = new HiveObjectSpec("test_db", "test_table", "ds=1/hr=3");

    Partition srcPartition1 =
        ReplicationTestUtils.createPartition(conf, srcMetastore, partitionSpec1);
    Partition srcPartition2 =
        ReplicationTestUtils.createPartition(conf, srcMetastore, partitionSpec2);
    Partition srcPartition3 =
        ReplicationTestUtils.createPartition(conf, srcMetastore, partitionSpec3);

    Map<HiveObjectSpec, Partition> specToPartition = new HashMap<>();

    specToPartition.put(partitionSpec1, srcPartition1);
    specToPartition.put(partitionSpec2, srcPartition2);
    specToPartition.put(partitionSpec3, srcPartition3);

    List<String> partitionNames = new ArrayList<>();
    partitionNames.add("ds=1/hr=1");
    partitionNames.add("ds=1/hr=2");
    partitionNames.add("ds=1/hr=3");

    // Find the common path for these partitions
    Optional<Path> commonDirectory =
        CopyPartitionsTask.findCommonDirectory(tableSpec, specToPartition);

    // Copy the partition
    CopyPartitionsTask copyPartitionsTask =
        new CopyPartitionsTask(conf, destinationObjectFactory, conflictHandler, srcCluster,
            destCluster, tableSpec, partitionNames, commonDirectory, jobExecutor, directoryCopier);
    RunInfo status = copyPartitionsTask.runTask();

    // Verify that the partition got copied
    assertEquals(RunInfo.RunStatus.SUCCESSFUL, status.getRunStatus());
    assertTrue(ReplicationUtils.exists(destMetastore, partitionSpec1));
    assertTrue(ReplicationUtils.exists(destMetastore, partitionSpec2));
    assertTrue(ReplicationUtils.exists(destMetastore, partitionSpec3));
    assertEquals(27, status.getBytesCopied());
  }

  /**
   * This ensures that the common directory isn't copied in cases where copying the common directory
   * would copy too much data.
   *
   * @throws IOException if there's an error writing to the local file system
   * @throws HiveMetastoreException if there's an error querying the metastore
   * @throws DistCpException if there's an error copying data
   */
  @Test
  public void testCopyPartitionsWithoutCopyingCommon()
      throws ConfigurationException, IOException, HiveMetastoreException, DistCpException {
    // Create a partitioned table in the source
    HiveObjectSpec tableSpec = new HiveObjectSpec("test_db", "test_table");
    ReplicationTestUtils.createPartitionedTable(conf, srcMetastore, tableSpec,
        TableType.MANAGED_TABLE, srcWarehouseRoot);

    // Create several partitions in the source table
    HiveObjectSpec partitionSpec1 = new HiveObjectSpec("test_db", "test_table", "ds=1/hr=1");
    HiveObjectSpec partitionSpec2 = new HiveObjectSpec("test_db", "test_table", "ds=1/hr=2");
    HiveObjectSpec partitionSpec3 = new HiveObjectSpec("test_db", "test_table", "ds=1/hr=3");
    HiveObjectSpec partitionSpec4 = new HiveObjectSpec("test_db", "test_table", "ds=1/hr=4");
    HiveObjectSpec partitionSpec5 = new HiveObjectSpec("test_db", "test_table", "ds=1/hr=5");

    Partition srcPartition1 =
        ReplicationTestUtils.createPartition(conf, srcMetastore, partitionSpec1);
    Partition srcPartition2 =
        ReplicationTestUtils.createPartition(conf, srcMetastore, partitionSpec2);
    Partition srcPartition3 =
        ReplicationTestUtils.createPartition(conf, srcMetastore, partitionSpec3);
    Partition srcPartition4 =
        ReplicationTestUtils.createPartition(conf, srcMetastore, partitionSpec4);
    Partition srcPartition5 =
        ReplicationTestUtils.createPartition(conf, srcMetastore, partitionSpec5);

    Map<HiveObjectSpec, Partition> specToPartition = new HashMap<>();

    specToPartition.put(partitionSpec1, srcPartition1);
    specToPartition.put(partitionSpec2, srcPartition2);
    specToPartition.put(partitionSpec3, srcPartition3);
    specToPartition.put(partitionSpec4, srcPartition4);
    specToPartition.put(partitionSpec3, srcPartition5);

    // Copy only two partitions
    List<String> partitionNames = new ArrayList<>();
    partitionNames.add("ds=1/hr=1");
    partitionNames.add("ds=1/hr=2");

    // Find the common path for these partitions
    Optional<Path> commonDirectory =
        CopyPartitionsTask.findCommonDirectory(tableSpec, specToPartition);

    ParallelJobExecutor jobExecutor = new ParallelJobExecutor(1);
    jobExecutor.start();

    // Copy the partition
    CopyPartitionsTask copyPartitionsTask =
        new CopyPartitionsTask(conf, destinationObjectFactory, conflictHandler, srcCluster,
            destCluster, tableSpec, partitionNames, commonDirectory, jobExecutor, directoryCopier);
    RunInfo status = copyPartitionsTask.runTask();

    // Verify that the partition got copied
    assertEquals(RunInfo.RunStatus.SUCCESSFUL, status.getRunStatus());
    assertTrue(ReplicationUtils.exists(destMetastore, partitionSpec1));
    assertTrue(ReplicationUtils.exists(destMetastore, partitionSpec2));
    assertFalse(ReplicationUtils.exists(destMetastore, partitionSpec3));
    assertFalse(ReplicationUtils.exists(destMetastore, partitionSpec4));
    assertFalse(ReplicationUtils.exists(destMetastore, partitionSpec5));
    assertEquals(18, status.getBytesCopied());
  }
}
