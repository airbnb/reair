package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.airbnb.reair.common.DistCpException;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.common.HiveParameterKeys;
import com.airbnb.reair.incremental.DirectoryCopier;
import com.airbnb.reair.incremental.ReplicationUtils;
import com.airbnb.reair.incremental.RunInfo;
import com.airbnb.reair.incremental.configuration.ConfigurationException;
import com.airbnb.reair.incremental.deploy.ConfigurationKeys;
import com.airbnb.reair.incremental.primitives.CopyPartitionTask;
import com.airbnb.reair.incremental.primitives.CopyPartitionedTableTask;
import com.airbnb.reair.incremental.primitives.CopyUnpartitionedTableTask;
import com.airbnb.reair.incremental.primitives.TaskEstimate;
import com.airbnb.reair.incremental.primitives.TaskEstimator;
import com.airbnb.reair.utils.ReplicationTestUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;
import java.util.Optional;

public class TaskEstimatorTest extends MockClusterTest {
  private static final Log LOG = LogFactory.getLog(TaskEstimatorTest.class);

  // Common names to use for testing
  private static final String HIVE_DB = "test_db";
  private static final String HIVE_TABLE = "test_table";
  private static final String HIVE_PARTITION = "ds=1/hr=1";

  @BeforeClass
  public static void setupClass() throws IOException, SQLException {
    MockClusterTest.setupClass();
  }

  @Test
  public void testEstimatesForUnpartitionedTable()
      throws ConfigurationException, IOException, HiveMetastoreException, DistCpException {

    final DirectoryCopier directoryCopier =
        new DirectoryCopier(conf, srcCluster.getTmpDir(), false);

    // Create an unpartitioned table in the source
    final HiveObjectSpec spec = new HiveObjectSpec(HIVE_DB, HIVE_TABLE);

    final Table srcTable = ReplicationTestUtils.createUnpartitionedTable(conf, srcMetastore, spec,
        TableType.MANAGED_TABLE, srcWarehouseRoot);

    final TaskEstimator estimator =
        new TaskEstimator(conf, destinationObjectFactory, srcCluster, destCluster, directoryCopier);

    // Table exists in source, but not in dest. It should copy the table.
    TaskEstimate estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.COPY_UNPARTITIONED_TABLE);
    assertTrue(estimate.isUpdateMetadata());
    assertTrue(estimate.isUpdateData());
    assertTrue(estimate.getSrcPath().get().equals(new Path(srcTable.getSd().getLocation())));

    // Replicate the table
    final CopyUnpartitionedTableTask copyJob =
        new CopyUnpartitionedTableTask(conf, destinationObjectFactory, conflictHandler, srcCluster,
            destCluster, spec, ReplicationUtils.getLocation(srcTable), directoryCopier, true);
    final RunInfo status = copyJob.runTask();
    assertEquals(RunInfo.RunStatus.SUCCESSFUL, status.getRunStatus());

    // A copy has been made on the destination. Now it shouldn't need to do
    // anything.
    estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.NO_OP);

    // Change the the source metadata. It should now require a metadata
    // update.
    srcTable.getParameters().put("foo", "bar");
    srcMetastore.alterTable(HIVE_DB, HIVE_TABLE, srcTable);
    estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.COPY_UNPARTITIONED_TABLE);
    assertTrue(estimate.isUpdateMetadata());
    assertFalse(estimate.isUpdateData());

    // Change the source data. It should now require a data update as well.
    ReplicationTestUtils.createTextFile(conf, new Path(srcTable.getSd().getLocation()),
        "new_file.txt", "456");
    estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.COPY_UNPARTITIONED_TABLE);
    assertTrue(estimate.isUpdateMetadata());
    assertTrue(estimate.isUpdateData());
    assertTrue(estimate.getSrcPath().get().equals(new Path(srcTable.getSd().getLocation())));

    // Drop the source. It should now be a drop.
    srcMetastore.dropTable(HIVE_DB, HIVE_TABLE, true);
    estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.DROP_TABLE);
  }

  @Test
  public void testEstimatesForPartitionedTable()
      throws IOException, HiveMetastoreException, DistCpException {

    final DirectoryCopier directoryCopier =
        new DirectoryCopier(conf, srcCluster.getTmpDir(), false);

    // Create an partitioned table in the source
    final HiveObjectSpec spec = new HiveObjectSpec(HIVE_DB, HIVE_TABLE);

    final Table srcTable = ReplicationTestUtils.createPartitionedTable(conf, srcMetastore, spec,
        TableType.MANAGED_TABLE, srcWarehouseRoot);

    final TaskEstimator estimator =
        new TaskEstimator(conf, destinationObjectFactory, srcCluster, destCluster, directoryCopier);

    // Table exists in source, but not in dest. It should copy the table.
    TaskEstimate estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.COPY_PARTITIONED_TABLE);
    assertTrue(estimate.isUpdateMetadata());
    assertFalse(estimate.isUpdateData());

    // Replicate the table
    final CopyPartitionedTableTask copyJob =
        new CopyPartitionedTableTask(conf, destinationObjectFactory, conflictHandler, srcCluster,
                                     destCluster, spec, ReplicationUtils.getLocation(srcTable));
    final RunInfo status = copyJob.runTask();
    assertEquals(RunInfo.RunStatus.SUCCESSFUL, status.getRunStatus());

    // A copy has been made on the destination. Now it shouldn't need to do
    // anything.
    estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.NO_OP);

    // Change the the source metadata. It should now require a metadata
    // update.
    srcTable.getParameters().put("foo", "bar");
    srcMetastore.alterTable(HIVE_DB, HIVE_TABLE, srcTable);
    estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.COPY_PARTITIONED_TABLE);
    assertTrue(estimate.isUpdateMetadata());
    assertFalse(estimate.isUpdateData());

    // Drop the source. It should now be a drop.
    srcMetastore.dropTable(HIVE_DB, HIVE_TABLE, true);
    estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.DROP_TABLE);
  }

  @Test
  public void testEstimatesForPartition()
      throws ConfigurationException, IOException, HiveMetastoreException, DistCpException {

    final DirectoryCopier directoryCopier =
        new DirectoryCopier(conf, srcCluster.getTmpDir(), false);

    // Create an partitioned table in the source
    final HiveObjectSpec tableSpec = new HiveObjectSpec(HIVE_DB, HIVE_TABLE);
    final Table srcTable =
        ReplicationTestUtils.createPartitionedTable(conf, srcMetastore, tableSpec,
                                                    TableType.MANAGED_TABLE, srcWarehouseRoot);

    // Create a partition in the source
    final HiveObjectSpec spec = new HiveObjectSpec(HIVE_DB, HIVE_TABLE, HIVE_PARTITION);
    final Partition srcPartition = ReplicationTestUtils.createPartition(conf, srcMetastore, spec);

    TaskEstimator estimator =
        new TaskEstimator(conf, destinationObjectFactory, srcCluster, destCluster, directoryCopier);

    // Partition exists in source, but not in dest. It should copy the
    // partition.
    TaskEstimate estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.COPY_PARTITION);
    assertTrue(estimate.isUpdateMetadata());
    assertTrue(estimate.isUpdateData());
    assertTrue(estimate.getSrcPath().get().equals(new Path(srcPartition.getSd().getLocation())));

    // Replicate the partition
    final CopyPartitionTask copyJob = new CopyPartitionTask(conf, destinationObjectFactory,
        conflictHandler, srcCluster, destCluster, spec, ReplicationUtils.getLocation(srcTable),
        Optional.<Path>empty(), directoryCopier, true);
    final RunInfo status = copyJob.runTask();
    assertEquals(RunInfo.RunStatus.SUCCESSFUL, status.getRunStatus());

    // A copy has been made on the destination. Now it shouldn't need to do
    // anything.
    estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.NO_OP);

    // Change the the source metadata. It should now require a metadata
    // update.
    srcPartition.getParameters().put("foo", "bar");
    srcMetastore.alterPartition(HIVE_DB, HIVE_TABLE, srcPartition);
    estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.COPY_PARTITION);
    assertTrue(estimate.isUpdateMetadata());
    assertFalse(estimate.isUpdateData());

    // Change the source data. It should now require a data update as well.
    ReplicationTestUtils.createTextFile(conf, new Path(srcPartition.getSd().getLocation()),
        "new_file.txt", "456");
    estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.COPY_PARTITION);
    assertTrue(estimate.isUpdateMetadata());
    assertTrue(estimate.isUpdateData());
    assertTrue(estimate.getSrcPath().get().equals(new Path(srcPartition.getSd().getLocation())));

    // Drop the source. It should now be a drop.
    srcMetastore.dropTable(HIVE_DB, HIVE_TABLE, true);
    estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.DROP_PARTITION);
  }

  @Test
  public void testEstimatesForUnpartitionedTableOverwriteNewer()
      throws IOException, HiveMetastoreException, DistCpException {

    // Overriding the default configuration, and make overwrite_newer = false.
    YarnConfiguration conf = new YarnConfiguration(MockClusterTest.conf);
    conf.set(ConfigurationKeys.BATCH_JOB_OVERWRITE_NEWER, Boolean.FALSE.toString());

    final DirectoryCopier directoryCopier =
        new DirectoryCopier(conf, srcCluster.getTmpDir(), false);

    // Create an unpartitioned table in the source
    final HiveObjectSpec spec = new HiveObjectSpec(HIVE_DB, HIVE_TABLE);

    final Table srcTable = ReplicationTestUtils.createUnpartitionedTable(conf, srcMetastore, spec,
        TableType.MANAGED_TABLE, srcWarehouseRoot);

    final long srcLmt = ReplicationUtils.getLastModifiedTime(srcTable);

    // Create an unpartitioned table in the destination that is newer
    final Table destTable = ReplicationTestUtils.createUnpartitionedTable(conf, destMetastore, spec,
        TableType.MANAGED_TABLE, destWarehouseRoot);
    destTable.putToParameters(HiveParameterKeys.TLDT, Long.toString(srcLmt + 1));
    destMetastore.alterTable(HIVE_DB, HIVE_TABLE, destTable);

    // Confirm that we won't overwrite newer tables
    final TaskEstimator estimator =
        new TaskEstimator(conf, destinationObjectFactory, srcCluster, destCluster, directoryCopier);
    TaskEstimate estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.NO_OP);

    // Modify the dest table to be older
    destTable.putToParameters(HiveParameterKeys.TLDT, Long.toString(srcLmt - 1));
    destMetastore.alterTable(HIVE_DB, HIVE_TABLE, destTable);

    // Confirm that we will still overwrite older tables
    TaskEstimate estimate2 = estimator.analyze(spec);
    assertTrue(estimate2.getTaskType() == TaskEstimate.TaskType.COPY_UNPARTITIONED_TABLE);
    assertTrue(estimate2.isUpdateMetadata());
    assertTrue(estimate2.getSrcPath().get().equals(new Path(srcTable.getSd().getLocation())));

    // Drop the source and destination
    srcMetastore.dropTable(HIVE_DB, HIVE_TABLE, true);
    destMetastore.dropTable(HIVE_DB, HIVE_TABLE, true);
  }

  @Test
  public void testEstimatesForPartitionOverwriteNewer()
      throws IOException, HiveMetastoreException, DistCpException {

    // Overriding the default configuration, and make overwrite_newer = false.
    YarnConfiguration conf = new YarnConfiguration(MockClusterTest.conf);
    conf.set(ConfigurationKeys.BATCH_JOB_OVERWRITE_NEWER, Boolean.FALSE.toString());

    final DirectoryCopier directoryCopier =
        new DirectoryCopier(conf, srcCluster.getTmpDir(), false);

    // Create an unpartitioned table in the source
    final HiveObjectSpec tableSpec = new HiveObjectSpec(HIVE_DB, HIVE_TABLE);
    final Table srcTable =
        ReplicationTestUtils.createPartitionedTable(conf, srcMetastore, tableSpec,
            TableType.MANAGED_TABLE, srcWarehouseRoot);

    // Create a partition in the source
    final HiveObjectSpec spec = new HiveObjectSpec(HIVE_DB, HIVE_TABLE, HIVE_PARTITION);
    final Partition srcPartition = ReplicationTestUtils.createPartition(conf, srcMetastore, spec);

    final long srcLmt = ReplicationUtils.getLastModifiedTime(srcPartition);

    // Create an partitioned table in the destination that is newer
    final Table destTable =
        ReplicationTestUtils.createPartitionedTable(conf, destMetastore, tableSpec,
            TableType.MANAGED_TABLE, destWarehouseRoot);
    final Partition destPartition = ReplicationTestUtils.createPartition(conf, destMetastore, spec);

    destPartition.putToParameters(HiveParameterKeys.TLDT, Long.toString(srcLmt + 1));
    destMetastore.alterPartition(HIVE_DB, HIVE_TABLE, destPartition);

    // Confirm that we won't overwrite newer partitions
    final TaskEstimator estimator =
        new TaskEstimator(conf, destinationObjectFactory, srcCluster, destCluster, directoryCopier);
    TaskEstimate estimate = estimator.analyze(spec);
    assertTrue(estimate.getTaskType() == TaskEstimate.TaskType.NO_OP);

    // Modify the dest partition to be older
    destPartition.putToParameters(HiveParameterKeys.TLDT, Long.toString(srcLmt - 1));
    destMetastore.alterPartition(HIVE_DB, HIVE_TABLE, destPartition);

    // Confirm that we will still overwrite older partitions
    TaskEstimate estimate2 = estimator.analyze(spec);
    assertTrue(estimate2.getTaskType() == TaskEstimate.TaskType.COPY_PARTITION);
    assertTrue(estimate2.isUpdateMetadata());
    assertTrue(estimate2.getSrcPath().get().equals(new Path(srcPartition.getSd().getLocation())));

    // Drop the source and destination
    srcMetastore.dropTable(HIVE_DB, HIVE_TABLE, true);
    destMetastore.dropTable(HIVE_DB, HIVE_TABLE, true);
  }

}

