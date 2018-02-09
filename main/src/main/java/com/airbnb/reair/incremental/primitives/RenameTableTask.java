package com.airbnb.reair.incremental.primitives;

import com.airbnb.reair.common.DistCpException;
import com.airbnb.reair.common.HiveMetastoreClient;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
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
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.util.Optional;

public class RenameTableTask implements ReplicationTask {
  private static final Log LOG = LogFactory.getLog(RenameTableTask.class);

  private Configuration conf;
  private DestinationObjectFactory destObjectFactory;
  private ObjectConflictHandler objectConflictHandler;
  private Cluster srcCluster;
  private Cluster destCluster;
  private HiveObjectSpec renameFromSpec;
  private HiveObjectSpec renameToSpec;
  private Optional<Path> renameFromPath;
  private Optional<Path> renameToPath;
  private Optional<String> renameFromTableTdlt;

  private ParallelJobExecutor copyPartitionsExecutor;
  private DirectoryCopier directoryCopier;

  /**
   * TODO.
   *
   * @param conf TODO
   * @param srcCluster TODO
   * @param destCluster TODO
   * @param destObjectFactory TODO
   * @param objectConflictHandler TODO
   * @param renameFromSpec TODO
   * @param renameToSpec TODO
   * @param renameFromPath TODO
   * @param renameToPath TODO
   * @param renameFromTableTldt TODO
   * @param copyPartitionsExecutor TODO
   * @param directoryCopier TODO
   */
  public RenameTableTask(
      Configuration conf,
      Cluster srcCluster,
      Cluster destCluster,
      DestinationObjectFactory destObjectFactory,
      ObjectConflictHandler objectConflictHandler,
      HiveObjectSpec renameFromSpec,
      HiveObjectSpec renameToSpec,
      Optional<Path> renameFromPath,
      Optional<Path> renameToPath,
      Optional<String> renameFromTableTldt,
      ParallelJobExecutor copyPartitionsExecutor,
      DirectoryCopier directoryCopier) {
    this.conf = conf;
    this.destObjectFactory = destObjectFactory;
    this.objectConflictHandler = objectConflictHandler;
    this.srcCluster = srcCluster;
    this.destCluster = destCluster;
    this.renameFromSpec = renameFromSpec;
    this.renameToSpec = renameToSpec;
    this.renameFromPath = renameFromPath;
    this.renameToPath = renameToPath;
    this.renameFromTableTdlt = renameFromTableTldt;

    this.copyPartitionsExecutor = copyPartitionsExecutor;
    this.directoryCopier = directoryCopier;
  }

  enum HandleRenameAction {
    RENAME_TABLE, COPY_TABLE, NO_OP
  }

  @Override
  public RunInfo runTask()
      throws ConfigurationException, DistCpException, HiveMetastoreException, IOException {
    LOG.debug("Renaming " + renameFromSpec + " to " + renameToSpec);

    HiveMetastoreClient destMs = destCluster.getMetastoreClient();
    HiveMetastoreClient srcMs = srcCluster.getMetastoreClient();

    Table freshSrcRenameToTable =
        srcMs.getTable(renameToSpec.getDbName(), renameToSpec.getTableName());

    Table freshDestRenameToTable =
        destMs.getTable(renameToSpec.getDbName(), renameToSpec.getTableName());

    // Get a fresh copy of the metadata from the dest Hive metastore
    Table freshDestTable =
        destMs.getTable(renameFromSpec.getDbName(), renameFromSpec.getTableName());

    if (!renameFromTableTdlt.isPresent()) {
      LOG.error(
          "For safety, not completing rename task since source " + " object TLDT is missing!");
      return new RunInfo(RunInfo.RunStatus.NOT_COMPLETABLE, 0);
    }

    String expectedTldt = renameFromTableTdlt.get();

    HandleRenameAction renameAction = null;
    if (ReplicationUtils.transientLastDdlTimesMatch(freshSrcRenameToTable,
        freshDestRenameToTable)) {
      LOG.debug(
          "Rename to table exists on destination and has a " + "matching TLDT. Not doing anything");
      renameAction = HandleRenameAction.NO_OP;

    } else if (freshDestRenameToTable != null) {
      LOG.debug("Rename to table already exists on destination, but "
          + "doesn't have a matching TLDT. Copying instead...");
      renameAction = HandleRenameAction.COPY_TABLE;
    } else if (freshDestTable == null) {
      LOG.warn(StringUtils.format("Destination rename from table %s " + "doesn't exist. "
          + "Copying %s to destination instead.", renameFromSpec, renameToSpec));
      renameAction = HandleRenameAction.COPY_TABLE;
    } else if (!ReplicationUtils.transientLastDdlTimesMatch(expectedTldt, freshDestTable)) {
      LOG.warn(StringUtils.format(
          "Destination table %s doesn't have " + "the expected modified time", renameFromSpec));
      LOG.debug("Renamed from source table with a TLDT: " + expectedTldt);
      LOG.debug("Table on destination: " + freshDestTable);
      LOG.debug(String.format("Copying %s to destination instead", renameToSpec));
      renameAction = HandleRenameAction.COPY_TABLE;
    } else {
      LOG.debug(String.format("Destination table (%s) matches " + "expected TLDT(%s) - will rename",
          renameFromSpec, ReplicationUtils.getTldt(freshDestTable)));
      renameAction = HandleRenameAction.RENAME_TABLE;
    }

    switch (renameAction) {
      case NO_OP:
        return new RunInfo(RunInfo.RunStatus.SUCCESSFUL, 0);

      case RENAME_TABLE:
        LOG.debug(StringUtils.format("Renaming %s to %s", renameFromSpec, renameToSpec));
        Table newTableOnDestination = new Table(freshDestTable);
        newTableOnDestination.setDbName(renameToSpec.getDbName());
        newTableOnDestination.setTableName(renameToSpec.getTableName());
        destMs.alterTable(renameFromSpec.getDbName(), renameFromSpec.getTableName(),
            newTableOnDestination);
        // After a rename, the table should be re-copied to get the
        // correct modified time changes. With a proper rename, this
        // should be a mostly no-op.
        LOG.debug(StringUtils.format("Renamed %s to %s", renameFromSpec, renameToSpec));
      // fallthrough to run the table copy as per above comment.
      case COPY_TABLE:
        CopyCompleteTableTask task =
            new CopyCompleteTableTask(conf, destObjectFactory, objectConflictHandler, srcCluster,
                destCluster, renameToSpec, renameToPath, copyPartitionsExecutor, directoryCopier);
        return task.runTask();

      default:
        throw new RuntimeException("Unhandled case: " + renameAction);
    }
  }

  @Override
  public LockSet getRequiredLocks() {
    LockSet lockSet = new LockSet();
    lockSet.add(new Lock(Lock.Type.EXCLUSIVE, renameFromSpec.toString()));
    lockSet.add(new Lock(Lock.Type.EXCLUSIVE, renameToSpec.toString()));
    return lockSet;
  }
}
