package test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.airbnb.reair.common.DistCpException;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.incremental.ReplicationUtils;
import com.airbnb.reair.incremental.RunInfo;
import com.airbnb.reair.incremental.configuration.ConfigurationException;
import com.airbnb.reair.incremental.primitives.CopyPartitionTask;
import com.airbnb.reair.incremental.primitives.DropPartitionTask;
import com.airbnb.reair.utils.ReplicationTestUtils;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;

import java.io.IOException;
import java.util.Optional;

public class DropPartitionTest extends MockClusterTest {
  @Test
  public void testDrop()
      throws ConfigurationException, DistCpException, HiveMetastoreException, IOException {
    String dbName = "test_db";
    String tableName = "test_Table";
    // Create a partitioned table in the source
    HiveObjectSpec tableSpec = new HiveObjectSpec(dbName, tableName);
    Table srcTable = ReplicationTestUtils.createPartitionedTable(conf, srcMetastore, tableSpec,
        TableType.MANAGED_TABLE, srcWarehouseRoot);

    // Create a partition in the source table
    String partitionName = "ds=1/hr=2";
    HiveObjectSpec partitionSpec = new HiveObjectSpec(dbName, tableName, partitionName);
    Partition srcPartition =
        ReplicationTestUtils.createPartition(conf, srcMetastore, partitionSpec);

    // Copy the partition
    CopyPartitionTask copyPartitionTask = new CopyPartitionTask(conf, destinationObjectFactory,
        conflictHandler, srcCluster, destCluster, partitionSpec,
        ReplicationUtils.getLocation(srcPartition), Optional.empty(), directoryCopier, true);
    RunInfo status = copyPartitionTask.runTask();

    // Verify that the table exists on the destination
    assertTrue(destMetastore.existsTable(dbName, tableName));

    // Pretend that a drop operation needs to be performed
    DropPartitionTask dropPartitionTask = new DropPartitionTask(srcCluster, destCluster,
        partitionSpec, ReplicationUtils.getTldt(srcPartition));
    dropPartitionTask.runTask();

    // Verify that the table exists, but the partition doest
    assertTrue(destMetastore.existsTable(dbName, tableName));
    assertFalse(destMetastore.existsPartition(dbName, tableName, partitionName));
    // Create a different partition on the destination, but with the same name
    Partition destPartition =
        ReplicationTestUtils.createPartition(conf, destMetastore, partitionSpec);

    // Pretend that a drop operation needs to be performed
    dropPartitionTask.runTask();

    // Verify that the partition still exists on the destination
    assertTrue(destMetastore.existsPartition(dbName, tableName, partitionName));
  }
}
