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
import com.airbnb.reair.incremental.primitives.CopyPartitionTask;
import com.airbnb.reair.incremental.primitives.RenamePartitionTask;
import com.airbnb.reair.multiprocessing.ParallelJobExecutor;
import com.airbnb.reair.utils.ReplicationTestUtils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class RenamePartitionTaskTest extends MockClusterTest {
  private static ParallelJobExecutor jobExecutor = new ParallelJobExecutor(1);

  /**
   * Sets up this class for testing.
   *
   * @throws IOException if there's an error accessing the local filesystem
   * @throws SQLException if there's an error querying the embedded DB
   */
  @BeforeClass
  public static void setupClass() throws IOException, SQLException {
    MockClusterTest.setupClass();
    jobExecutor.start();
  }

  @Test
  public void testRenamePartition()
      throws ConfigurationException, IOException, HiveMetastoreException, DistCpException {
    final String dbName = "test_db";
    final String tableName = "test_table";
    final String oldPartitionName = "ds=1/hr=1";
    final String newPartitionName = "ds=1/hr=2";

    // Create an partitioned table in the source
    final HiveObjectSpec originalTableSpec = new HiveObjectSpec(dbName, tableName);
    final HiveObjectSpec oldPartitionSpec = new HiveObjectSpec(dbName, tableName, oldPartitionName);
    final HiveObjectSpec newPartitionSpec = new HiveObjectSpec(dbName, tableName, newPartitionName);

    ReplicationTestUtils.createPartitionedTable(conf, srcMetastore,
        originalTableSpec, TableType.MANAGED_TABLE, srcWarehouseRoot);

    final Partition oldPartition =
        ReplicationTestUtils.createPartition(conf, srcMetastore, oldPartitionSpec);

    // Copy the partition
    final Configuration testConf = new Configuration(conf);
    final CopyPartitionTask copyJob = new CopyPartitionTask(testConf, destinationObjectFactory,
        conflictHandler, srcCluster, destCluster, oldPartitionSpec,
        ReplicationUtils.getLocation(oldPartition), Optional.empty(), directoryCopier, true);

    copyJob.runTask();

    // Rename the source partition
    final Partition newPartition = new Partition(oldPartition);
    final List<String> newValues = new ArrayList<>();
    newValues.add("1");
    newValues.add("2");
    newPartition.setValues(newValues);

    srcMetastore.renamePartition(dbName, tableName, oldPartition.getValues(), newPartition);

    // Propagate the rename
    final RenamePartitionTask task = new RenamePartitionTask(testConf, destinationObjectFactory,
        conflictHandler, srcCluster, destCluster, oldPartitionSpec, newPartitionSpec,
        ReplicationUtils.getLocation(oldPartition), ReplicationUtils.getLocation(newPartition),
        ReplicationUtils.getTldt(oldPartition), directoryCopier);

    final RunInfo runInfo = task.runTask();

    // Check to make sure that the rename has succeeded
    assertEquals(RunInfo.RunStatus.SUCCESSFUL, runInfo.getRunStatus());
    assertTrue(destMetastore.existsPartition(newPartitionSpec.getDbName(),
        newPartitionSpec.getTableName(), newPartitionSpec.getPartitionName()));
    assertFalse(destMetastore.existsPartition(oldPartitionSpec.getDbName(),
        oldPartitionSpec.getTableName(), oldPartitionSpec.getPartitionName()));
    assertEquals(ReplicationTestUtils.getModifiedTime(srcMetastore, newPartitionSpec),
        ReplicationTestUtils.getModifiedTime(destMetastore, newPartitionSpec));
  }

  @Test
  public void testRenamePartitionByThrift()
      throws ConfigurationException, IOException, HiveMetastoreException, DistCpException {
    final String dbName = "test_db";
    final String tableName = "test_table";
    final String oldPartitionName = "ds=1/hr=1";
    final String newPartitionName = "ds=1/hr=2";

    // Create an partitioned table in the source
    final HiveObjectSpec originalTableSpec = new HiveObjectSpec(dbName, tableName);
    final HiveObjectSpec oldPartitionSpec = new HiveObjectSpec(dbName, tableName, oldPartitionName);
    final HiveObjectSpec newPartitionSpec = new HiveObjectSpec(dbName, tableName, newPartitionName);

    ReplicationTestUtils.createPartitionedTable(conf, srcMetastore,
        originalTableSpec, TableType.MANAGED_TABLE, srcWarehouseRoot);

    final Partition oldPartition =
        ReplicationTestUtils.createPartition(conf, srcMetastore, oldPartitionSpec);

    // Copy the partition
    final Configuration testConf = new Configuration(conf);
    final CopyPartitionTask copyJob = new CopyPartitionTask(testConf, destinationObjectFactory,
        conflictHandler, srcCluster, destCluster, oldPartitionSpec,
        ReplicationUtils.getLocation(oldPartition), Optional.empty(), directoryCopier, true);

    copyJob.runTask();

    // Rename the source partition
    final Partition newPartition = new Partition(oldPartition);
    final List<String> newValues = new ArrayList<>();
    newValues.add("1");
    newValues.add("2");
    newPartition.setValues(newValues);

    srcMetastore.renamePartition(dbName, tableName, oldPartition.getValues(), newPartition);

    // Propagate the rename
    final RenamePartitionTask task = new RenamePartitionTask(testConf, destinationObjectFactory,
        conflictHandler, srcCluster, destCluster, oldPartitionSpec, newPartitionSpec,
        ReplicationUtils.getLocation(oldPartition), ReplicationUtils.getLocation(newPartition),
        ReplicationUtils.getTldt(oldPartition), directoryCopier);

    final RunInfo runInfo = task.runTask();

    // Check to make sure that the rename has succeeded
    assertEquals(RunInfo.RunStatus.SUCCESSFUL, runInfo.getRunStatus());
    assertTrue(destMetastore.existsPartition(newPartitionSpec.getDbName(),
        newPartitionSpec.getTableName(), newPartitionSpec.getPartitionName()));
    assertFalse(destMetastore.existsPartition(oldPartitionSpec.getDbName(),
        oldPartitionSpec.getTableName(), oldPartitionSpec.getPartitionName()));
    assertEquals(ReplicationTestUtils.getModifiedTime(srcMetastore, newPartitionSpec),
        ReplicationTestUtils.getModifiedTime(destMetastore, newPartitionSpec));
  }
}
