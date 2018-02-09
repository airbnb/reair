package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.airbnb.reair.common.DistCpException;
import com.airbnb.reair.common.FsUtils;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.incremental.ReplicationUtils;
import com.airbnb.reair.incremental.RunInfo;
import com.airbnb.reair.incremental.configuration.ConfigurationException;
import com.airbnb.reair.incremental.primitives.CopyCompleteTableTask;
import com.airbnb.reair.multiprocessing.ParallelJobExecutor;
import com.airbnb.reair.utils.ReplicationTestUtils;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class CopyCompleteTableTaskTest extends MockClusterTest {

  private static ParallelJobExecutor jobExecutor = new ParallelJobExecutor(1);

  @BeforeClass
  public static void setupClass() throws IOException, SQLException {
    MockClusterTest.setupClass();
    jobExecutor.start();
  }

  @Test
  public void testCopyUnpartitionedTable()
      throws ConfigurationException, IOException, HiveMetastoreException, DistCpException {

    // Create an unpartitioned table in the source
    HiveObjectSpec spec = new HiveObjectSpec("test_db", "test_table");
    Table srcTable = ReplicationTestUtils.createUnpartitionedTable(conf, srcMetastore, spec,
        TableType.MANAGED_TABLE, srcWarehouseRoot);

    // Copy the table
    CopyCompleteTableTask copyJob = new CopyCompleteTableTask(conf, destinationObjectFactory,
        conflictHandler, srcCluster, destCluster, spec, ReplicationUtils.getLocation(srcTable),
        jobExecutor, directoryCopier);
    RunInfo status = copyJob.runTask();

    // Verify that the table exists on the destination, the location is
    // within the destination filesystem, the data is the same,
    // and the right number of bytes were copied.
    assertEquals(RunInfo.RunStatus.SUCCESSFUL, status.getRunStatus());
    Table destTable = destMetastore.getTable(spec.getDbName(), spec.getTableName());
    assertNotNull(destTable);
    assertTrue(destTable.getSd().getLocation().startsWith(destCluster.getFsRoot() + "/"));
    assertTrue(FsUtils.equalDirs(conf, new Path(srcTable.getSd().getLocation()),
        new Path(destTable.getSd().getLocation())));
    assertEquals(9, status.getBytesCopied());

    // Verify that doing a copy again is a no-op
    RunInfo rerunStatus = copyJob.runTask();
    assertEquals(RunInfo.RunStatus.SUCCESSFUL, rerunStatus.getRunStatus());
    assertEquals(0, rerunStatus.getBytesCopied());

  }

  @Test
  public void testCopyPartitionedTable()
      throws ConfigurationException, IOException, HiveMetastoreException, DistCpException {
    // Create a partitioned table in the source
    HiveObjectSpec tableSpec = new HiveObjectSpec("test_db", "test_table");
    Table srcTable = ReplicationTestUtils.createPartitionedTable(conf, srcMetastore, tableSpec,
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

    // Copy the partition
    CopyCompleteTableTask copyCompleteTableTask = new CopyCompleteTableTask(conf,
        destinationObjectFactory, conflictHandler, srcCluster, destCluster, tableSpec,
        ReplicationUtils.getLocation(srcTable), jobExecutor, directoryCopier);
    RunInfo status = copyCompleteTableTask.runTask();

    // Verify that the partition got copied
    assertEquals(RunInfo.RunStatus.SUCCESSFUL, status.getRunStatus());
    assertTrue(ReplicationUtils.exists(destMetastore, partitionSpec1));
    assertTrue(ReplicationUtils.exists(destMetastore, partitionSpec2));
    assertTrue(ReplicationUtils.exists(destMetastore, partitionSpec3));
    assertEquals(27, status.getBytesCopied());
  }
}
