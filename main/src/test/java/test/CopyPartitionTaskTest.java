package test;

import static org.junit.Assert.assertEquals;

import com.airbnb.reair.common.DistCpException;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.incremental.ReplicationUtils;
import com.airbnb.reair.incremental.RunInfo;
import com.airbnb.reair.incremental.configuration.ConfigurationException;
import com.airbnb.reair.incremental.primitives.CopyPartitionTask;
import com.airbnb.reair.utils.ReplicationTestUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

public class CopyPartitionTaskTest extends MockClusterTest {
  private static final Log LOG = LogFactory.getLog(CopyPartitionTaskTest.class);

  @Test
  public void testCopyPartition()
      throws ConfigurationException, IOException, HiveMetastoreException, DistCpException {
    // Create a partitioned table in the source
    HiveObjectSpec tableSpec = new HiveObjectSpec("test_db", "test_table");
    Table srcTable = ReplicationTestUtils.createPartitionedTable(conf, srcMetastore, tableSpec,
        TableType.MANAGED_TABLE, srcWarehouseRoot);

    // Create a partition in the source table
    HiveObjectSpec partitionSpec = new HiveObjectSpec("test_db", "test_table", "ds=1/hr=1");
    Partition srcPartition =
        ReplicationTestUtils.createPartition(conf, srcMetastore, partitionSpec);

    // Copy the partition
    CopyPartitionTask copyPartitionTask = new CopyPartitionTask(conf, destinationObjectFactory,
        conflictHandler, srcCluster, destCluster, partitionSpec,
        ReplicationUtils.getLocation(srcPartition), Optional.empty(), directoryCopier, true);
    RunInfo status = copyPartitionTask.runTask();

    // Verify that the partition got copied
    assertEquals(RunInfo.RunStatus.SUCCESSFUL, status.getRunStatus());
    assertEquals(9, status.getBytesCopied());

    // Copying a new partition without a data copy should not succeed.
    partitionSpec = new HiveObjectSpec("test_db", "test_table", "ds=1/hr=2");
    ReplicationTestUtils.createPartition(conf, srcMetastore, partitionSpec);
    copyPartitionTask = new CopyPartitionTask(conf, destinationObjectFactory, conflictHandler,
        srcCluster, destCluster, partitionSpec, ReplicationUtils.getLocation(srcPartition),
        Optional.<Path>empty(), directoryCopier, false);
    status = copyPartitionTask.runTask();
    assertEquals(RunInfo.RunStatus.NOT_COMPLETABLE, status.getRunStatus());
    assertEquals(0, status.getBytesCopied());
  }

  @Test
  public void testCopyPartitionView()
      throws ConfigurationException, IOException, HiveMetastoreException, DistCpException {
    // Create a partitioned table in the source
    HiveObjectSpec tableSpec = new HiveObjectSpec("test_db", "test_table_view");
    ReplicationTestUtils.createPartitionedTable(conf, srcMetastore, tableSpec,
        TableType.VIRTUAL_VIEW, srcWarehouseRoot);

    // Create a partition in the source table
    HiveObjectSpec partitionSpec = new HiveObjectSpec("test_db", "test_table_view", "ds=1/hr=1");
    Partition srcPartition =
        ReplicationTestUtils.createPartition(conf, srcMetastore, partitionSpec);

    // Copy the partition
    CopyPartitionTask copyPartitionTask = new CopyPartitionTask(conf, destinationObjectFactory,
        conflictHandler, srcCluster, destCluster, partitionSpec,
        ReplicationUtils.getLocation(srcPartition), Optional.<Path>empty(), directoryCopier, true);
    RunInfo status = copyPartitionTask.runTask();

    // Verify that the partition got copied
    assertEquals(RunInfo.RunStatus.SUCCESSFUL, status.getRunStatus());
    assertEquals(0, status.getBytesCopied());
  }
}
