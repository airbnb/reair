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
import com.airbnb.reair.incremental.primitives.CopyUnpartitionedTableTask;
import com.airbnb.reair.incremental.primitives.RenameTableTask;
import com.airbnb.reair.multiprocessing.ParallelJobExecutor;
import com.airbnb.reair.utils.ReplicationTestUtils;

import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.sql.SQLException;

public class RenameTableTaskTest extends MockClusterTest {

  private static ParallelJobExecutor jobExecutor = new ParallelJobExecutor(1);

  /**
   * Sets up this class before running tests.
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
  public void testRenameTable()
      throws ConfigurationException, IOException, HiveMetastoreException, DistCpException {
    final String dbName = "test_db";
    final String tableName = "test_table";
    final String newTableName = "new_test_table";

    // Create an unpartitioned table in the source
    final HiveObjectSpec originalTableSpec = new HiveObjectSpec(dbName, tableName);
    final Table srcTable = ReplicationTestUtils.createUnpartitionedTable(conf, srcMetastore,
        originalTableSpec, TableType.MANAGED_TABLE, srcWarehouseRoot);

    // Copy the table
    final CopyUnpartitionedTableTask copyJob = new CopyUnpartitionedTableTask(conf,
        destinationObjectFactory, conflictHandler, srcCluster, destCluster, originalTableSpec,
        ReplicationUtils.getLocation(srcTable), directoryCopier, true);
    final RunInfo status = copyJob.runTask();

    // Rename the source table
    final Table originalSrcTable = new Table(srcTable);
    srcTable.setTableName(newTableName);
    srcMetastore.alterTable(dbName, tableName, srcTable);
    final HiveObjectSpec newTableSpec = new HiveObjectSpec(dbName, newTableName);
    ReplicationTestUtils.updateModifiedTime(srcMetastore, newTableSpec);

    // Propagate the rename
    final RenameTableTask job = new RenameTableTask(conf, srcCluster, destCluster,
        destinationObjectFactory, conflictHandler, originalTableSpec, newTableSpec,
        ReplicationUtils.getLocation(originalSrcTable), ReplicationUtils.getLocation(srcTable),
        ReplicationUtils.getTldt(originalSrcTable), jobExecutor, directoryCopier);

    final RunInfo runInfo = job.runTask();

    // Check to make sure that the rename has succeeded
    assertEquals(RunInfo.RunStatus.SUCCESSFUL, runInfo.getRunStatus());
    assertTrue(destMetastore.existsTable(newTableSpec.getDbName(), newTableSpec.getTableName()));
    assertFalse(
        destMetastore.existsTable(originalTableSpec.getDbName(), originalTableSpec.getTableName()));
    assertEquals(ReplicationTestUtils.getModifiedTime(srcMetastore, newTableSpec),
        ReplicationTestUtils.getModifiedTime(destMetastore, newTableSpec));
  }

  @Test
  public void testRenameTableReqiringCopy()
      throws ConfigurationException, IOException, HiveMetastoreException, DistCpException {
    final String dbName = "test_db";
    final String tableName = "test_table";
    final String newTableName = "new_test_table";

    // Create an unpartitioned table in the source
    final HiveObjectSpec originalTableSpec = new HiveObjectSpec(dbName, tableName);
    final Table srcTable = ReplicationTestUtils.createUnpartitionedTable(conf, srcMetastore,
        originalTableSpec, TableType.MANAGED_TABLE, srcWarehouseRoot);


    // Rename the source table. Note that the source table wasn't copied to
    // the destination.
    final Table originalSrcTable = new Table(srcTable);
    srcTable.setTableName(newTableName);
    srcMetastore.alterTable(dbName, tableName, srcTable);
    final HiveObjectSpec newTableSpec = new HiveObjectSpec(dbName, newTableName);
    ReplicationTestUtils.updateModifiedTime(srcMetastore, newTableSpec);

    // Propagate the rename
    final RenameTableTask job = new RenameTableTask(conf, srcCluster, destCluster,
        destinationObjectFactory, conflictHandler, originalTableSpec, newTableSpec,
        ReplicationUtils.getLocation(originalSrcTable), ReplicationUtils.getLocation(srcTable),
        ReplicationUtils.getTldt(originalSrcTable), jobExecutor, directoryCopier);

    final RunInfo runInfo = job.runTask();

    // Check to make sure that the expected table exists has succeeded
    assertEquals(RunInfo.RunStatus.SUCCESSFUL, runInfo.getRunStatus());
    assertTrue(destMetastore.existsTable(newTableSpec.getDbName(), newTableSpec.getTableName()));
    assertFalse(
        destMetastore.existsTable(originalTableSpec.getDbName(), originalTableSpec.getTableName()));
  }
}
