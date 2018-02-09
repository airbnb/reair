package test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.airbnb.reair.common.DistCpException;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.incremental.ReplicationUtils;
import com.airbnb.reair.incremental.RunInfo;
import com.airbnb.reair.incremental.configuration.ConfigurationException;
import com.airbnb.reair.incremental.primitives.CopyUnpartitionedTableTask;
import com.airbnb.reair.incremental.primitives.DropTableTask;
import com.airbnb.reair.utils.ReplicationTestUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.Table;
import org.junit.Test;

import java.io.IOException;

public class DropTableTest extends MockClusterTest {
  private static final Log LOG = LogFactory.getLog(DropTableTest.class);

  @Test
  public void testDrop()
      throws ConfigurationException, DistCpException, HiveMetastoreException, IOException {
    String dbName = "test_db";
    String tableName = "test_Table";
    // Create an unpartitioned table in the source
    HiveObjectSpec spec = new HiveObjectSpec(dbName, tableName);
    Table srcTable = ReplicationTestUtils.createUnpartitionedTable(conf, srcMetastore, spec,
        TableType.MANAGED_TABLE, srcWarehouseRoot);

    // Copy the table
    CopyUnpartitionedTableTask copyJob =
        new CopyUnpartitionedTableTask(conf, destinationObjectFactory, conflictHandler, srcCluster,
            destCluster, spec, ReplicationUtils.getLocation(srcTable), directoryCopier, true);
    RunInfo status = copyJob.runTask();

    // Verify that the table exists on the destination
    assertTrue(destMetastore.existsTable(dbName, tableName));

    // Pretend that a drop operation needs to be performed
    DropTableTask dropTableTask =
        new DropTableTask(srcCluster, destCluster, spec, ReplicationUtils.getTldt(srcTable));
    dropTableTask.runTask();

    // Verify that the table doesn't exist on the destination
    assertFalse(destMetastore.existsTable(dbName, tableName));

    // Create a different table on the destination, but with the same name
    Table destTable = ReplicationTestUtils.createUnpartitionedTable(conf, destMetastore, spec,
        TableType.MANAGED_TABLE, destWarehouseRoot);

    // Pretend that a drop operation needs to be performed
    dropTableTask.runTask();

    // Verify that the table still exists on the destination
    assertTrue(destMetastore.existsTable(dbName, tableName));
  }
}
