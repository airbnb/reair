package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.airbnb.reair.common.DistCpException;
import com.airbnb.reair.common.FsUtils;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.incremental.ReplicationUtils;
import com.airbnb.reair.incremental.RunInfo;
import com.airbnb.reair.incremental.configuration.ConfigurationException;
import com.airbnb.reair.incremental.primitives.CopyUnpartitionedTableTask;
import com.airbnb.reair.utils.ReplicationTestUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;

import java.io.IOException;

public class CopyUnpartitionedTableTaskTest extends MockClusterTest {
  private static final Log LOG = LogFactory.getLog(CopyUnpartitionedTableTaskTest.class);

  @Test
  public void testCopyUnpartitionedTable()
      throws ConfigurationException, IOException, HiveMetastoreException, DistCpException {

    // Create an unpartitioned table in the source
    HiveObjectSpec spec = new HiveObjectSpec("test_db", "test_table");
    Table srcTable = ReplicationTestUtils.createUnpartitionedTable(conf, srcMetastore, spec,
        TableType.MANAGED_TABLE, srcWarehouseRoot);

    // Copy the table
    CopyUnpartitionedTableTask copyJob =
        new CopyUnpartitionedTableTask(conf, destinationObjectFactory, conflictHandler, srcCluster,
            destCluster, spec, ReplicationUtils.getLocation(srcTable), directoryCopier, true);
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

    // Trying to copy a new table without a data copy should not succeed.
    HiveObjectSpec spec2 = new HiveObjectSpec("test_db", "test_table_2");
    ReplicationTestUtils.createUnpartitionedTable(conf, srcMetastore, spec2,
        TableType.MANAGED_TABLE, srcWarehouseRoot);
    CopyUnpartitionedTableTask copyJob2 =
        new CopyUnpartitionedTableTask(conf, destinationObjectFactory, conflictHandler, srcCluster,
            destCluster, spec2, ReplicationUtils.getLocation(srcTable), directoryCopier, false);
    RunInfo status2 = copyJob2.runTask();

    // Verify that the table exists on the destination, the location is
    // within the destination filesystem, the data is the same,
    // and the right number of bytes were copied.
    assertEquals(RunInfo.RunStatus.NOT_COMPLETABLE, status2.getRunStatus());
  }

  @Test
  public void testCopyUnpartitionedTableView()
      throws ConfigurationException, IOException, HiveMetastoreException, DistCpException {
    HiveObjectSpec spec = new HiveObjectSpec("test_db", "test_table_view");
    Table srcTable = ReplicationTestUtils.createUnpartitionedTable(conf, srcMetastore, spec,
        TableType.VIRTUAL_VIEW, srcWarehouseRoot);

    // Copy the table
    CopyUnpartitionedTableTask copyJob =
        new CopyUnpartitionedTableTask(conf, destinationObjectFactory, conflictHandler, srcCluster,
            destCluster, spec, ReplicationUtils.getLocation(srcTable), directoryCopier, true);
    RunInfo status = copyJob.runTask();

    // Verify that the table exists on the destination, the location is
    // within the destination filesystem, the data is the same,
    // and the right number of bytes were copied.
    assertEquals(RunInfo.RunStatus.SUCCESSFUL, status.getRunStatus());
    Table destTable = destMetastore.getTable(spec.getDbName(), spec.getTableName());
    assertNotNull(destTable);
    assertNull(destTable.getSd().getLocation());
    assertEquals(0, status.getBytesCopied());
  }
}
