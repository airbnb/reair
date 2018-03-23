package com.airbnb.reair.incremental;

import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.common.HiveUtils;
import com.airbnb.reair.common.NamedPartition;
import com.airbnb.reair.hive.hooks.HiveOperation;
import com.airbnb.reair.incremental.auditlog.AuditLogEntry;
import com.airbnb.reair.incremental.configuration.Cluster;
import com.airbnb.reair.incremental.configuration.DestinationObjectFactory;
import com.airbnb.reair.incremental.configuration.ObjectConflictHandler;
import com.airbnb.reair.incremental.db.PersistedJobInfo;
import com.airbnb.reair.incremental.db.PersistedJobInfoStore;
import com.airbnb.reair.incremental.filter.ReplicationFilter;
import com.airbnb.reair.incremental.primitives.CopyPartitionTask;
import com.airbnb.reair.incremental.primitives.CopyPartitionedTableTask;
import com.airbnb.reair.incremental.primitives.CopyPartitionsTask;
import com.airbnb.reair.incremental.primitives.CopyUnpartitionedTableTask;
import com.airbnb.reair.incremental.primitives.DropPartitionTask;
import com.airbnb.reair.incremental.primitives.DropTableTask;
import com.airbnb.reair.incremental.primitives.RenamePartitionTask;
import com.airbnb.reair.incremental.primitives.RenameTableTask;
import com.airbnb.reair.incremental.primitives.ReplicationTask;
import com.airbnb.reair.multiprocessing.ParallelJobExecutor;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Creates replication jobs and persists initial information into the DB.
 */
public class ReplicationJobFactory {

  private static final Log LOG = LogFactory.getLog(ReplicationJobFactory.class);

  private Configuration conf;
  private Cluster srcCluster;
  private Cluster destCluster;
  private DestinationObjectFactory destinationObjectFactory;
  private OnStateChangeHandler onStateChangeHandler;
  private ObjectConflictHandler objectConflictHandler;
  private ParallelJobExecutor copyPartitionJobExecutor;
  private DirectoryCopier directoryCopier;
  private PersistedJobInfoStore persistedJobInfoStore;

  /**
   * Constructor.
   *
   * @param conf configuration
   * @param srcCluster source cluster
   * @param destCluster destination cluster
   * @param persistedJobInfoStore PersistedJobInfoStore
   * @param destinationObjectFactory factory for creating objects for the destination cluster
   * @param onStateChangeHandler handler for when a job's state changes
   * @param objectConflictHandler handler for addressing conflicting tables/partitions on the
   *                              destination cluster
   * @param copyPartitionJobExecutor executor for copying partitions
   * @param directoryCopier copies directories using MR jobs
   */
  public ReplicationJobFactory(
      Configuration conf,
      Cluster srcCluster,
      Cluster destCluster,
      PersistedJobInfoStore persistedJobInfoStore,
      DestinationObjectFactory destinationObjectFactory,
      OnStateChangeHandler onStateChangeHandler,
      ObjectConflictHandler objectConflictHandler,
      ParallelJobExecutor copyPartitionJobExecutor,
      DirectoryCopier directoryCopier) {
    this.conf = conf;
    this.srcCluster = srcCluster;
    this.destCluster = destCluster;
    this.destinationObjectFactory = destinationObjectFactory;
    this.onStateChangeHandler = onStateChangeHandler;
    this.objectConflictHandler = objectConflictHandler;
    this.copyPartitionJobExecutor = copyPartitionJobExecutor;
    this.directoryCopier = directoryCopier;
    this.persistedJobInfoStore = persistedJobInfoStore;
  }

  /**
   * Create a replication job to copy a table.
   *
   * @param auditLogId ID of the audit log entry where this table was created
   * @param auditLogEntryCreateTime when the audit log entry was created
   * @param table the table to copy
   * @return job to copy the table
   *
   * @throws StateUpdateException if there's an error writing to the DB
   */
  public ReplicationJob createJobForCopyTable(
      long auditLogId,
      long auditLogEntryCreateTime,
      Table table) throws StateUpdateException {
    ReplicationOperation replicationOperation =
        HiveUtils.isPartitioned(table) ? ReplicationOperation.COPY_PARTITIONED_TABLE
            : ReplicationOperation.COPY_UNPARTITIONED_TABLE;

    Map<String, String> extras = new HashMap<>();
    extras.put(PersistedJobInfo.AUDIT_LOG_ID_EXTRAS_KEY, Long.toString(auditLogId));
    extras.put(PersistedJobInfo.AUDIT_LOG_ENTRY_CREATE_TIME_KEY,
        Long.toString(auditLogEntryCreateTime));

    PersistedJobInfo persistedJobInfo = PersistedJobInfo.createDeferred(
        replicationOperation,
        ReplicationStatus.PENDING, ReplicationUtils.getLocation(table), srcCluster.getName(),
        new HiveObjectSpec(table), Collections.emptyList(), ReplicationUtils.getTldt(table),
        Optional.empty(), Optional.empty(), extras);

    HiveObjectSpec spec = new HiveObjectSpec(table);
    Optional<Path> tableLocation = ReplicationUtils.getLocation(table);

    switch (replicationOperation) {
      case COPY_UNPARTITIONED_TABLE:
        return new ReplicationJob(
            conf,
            new CopyUnpartitionedTableTask(conf, destinationObjectFactory, objectConflictHandler,
                srcCluster, destCluster, spec, tableLocation, directoryCopier, true),
            onStateChangeHandler, persistedJobInfo);
      case COPY_PARTITIONED_TABLE:
        return new ReplicationJob(
            conf,
            new CopyPartitionedTableTask(conf, destinationObjectFactory, objectConflictHandler,
                srcCluster, destCluster, spec, tableLocation),
            onStateChangeHandler, persistedJobInfo);
      default:
        throw new RuntimeException("Unhandled operation " + replicationOperation);
    }
  }

  /**
   * Create a replication job to copy a partition.
   *
   * @param auditLogId ID of the audit log entry where this partition was created
   * @param auditLogEntryCreateTime when the audit log entry was created
   * @param spec specification for the partition
   * @return the job to copy the partition
   *
   * @throws StateUpdateException if there's an error writing to the DB
   */
  public ReplicationJob createJobForCopyPartition(
      long auditLogId,
      long auditLogEntryCreateTime,
      HiveObjectSpec spec) throws StateUpdateException {

    Map<String, String> extras = new HashMap<>();
    extras.put(PersistedJobInfo.AUDIT_LOG_ID_EXTRAS_KEY, Long.toString(auditLogId));
    extras.put(PersistedJobInfo.AUDIT_LOG_ENTRY_CREATE_TIME_KEY,
        Long.toString(auditLogEntryCreateTime));

    List<String> partitionNames = new ArrayList<>();
    partitionNames.add(spec.getPartitionName());
    ReplicationOperation replicationOperation = ReplicationOperation.COPY_PARTITION;

    PersistedJobInfo persistedJobInfo =
        PersistedJobInfo.createDeferred(
            replicationOperation, ReplicationStatus.PENDING,
            Optional.empty(), srcCluster.getName(), spec, partitionNames,
            Optional.empty(), Optional.empty(), Optional.empty(), extras);

    ReplicationTask replicationTask = new CopyPartitionTask(conf, destinationObjectFactory,
        objectConflictHandler, srcCluster, destCluster, spec, Optional.<Path>empty(),
        Optional.<Path>empty(), directoryCopier, true);

    return new ReplicationJob(conf, replicationTask, onStateChangeHandler, persistedJobInfo);
  }

  /**
   * Create a replication job to copy a partition.
   *
   * @param auditLogId ID of the audit log entry where this partition was created
   * @param auditLogEntryCreateTime when the audit log entry was created
   * @param namedPartition partition to copy
   * @return the job to copy the partition
   *
   * @throws StateUpdateException if there's an error writing to the DB
   */
  public ReplicationJob createJobForCopyPartition(
      long auditLogId,
      long auditLogEntryCreateTime,
      NamedPartition namedPartition) throws StateUpdateException {
    String partitionName = namedPartition.getName();
    List<String> partitionNames = new ArrayList<>();
    partitionNames.add(partitionName);

    ReplicationOperation replicationOperation = ReplicationOperation.COPY_PARTITION;

    Map<String, String> extras = new HashMap<>();
    extras.put(PersistedJobInfo.AUDIT_LOG_ID_EXTRAS_KEY, Long.toString(auditLogId));
    extras.put(PersistedJobInfo.AUDIT_LOG_ENTRY_CREATE_TIME_KEY,
        Long.toString(auditLogEntryCreateTime));

    Partition partition = namedPartition.getPartition();
    HiveObjectSpec spec = new HiveObjectSpec(namedPartition);
    PersistedJobInfo persistedJobInfo =
        PersistedJobInfo.createDeferred(
            replicationOperation, ReplicationStatus.PENDING,
            ReplicationUtils.getLocation(partition), srcCluster.getName(), spec, partitionNames,
            ReplicationUtils.getTldt(partition), Optional.empty(), Optional.empty(), extras);

    ReplicationTask replicationTask = new CopyPartitionTask(
        conf, destinationObjectFactory, objectConflictHandler, srcCluster, destCluster, spec,
        ReplicationUtils.getLocation(partition), Optional.empty(), directoryCopier, true);

    return new ReplicationJob(conf, replicationTask, onStateChangeHandler, persistedJobInfo);
  }

  /**
   * Create a replication job to copy many partitions that were created by dynamic partitioning.
   *
   * @param auditLogId ID of the audit log entry where the partitions were created
   * @param auditLogEntryCreateTime when the audit log entry was created
   * @param namedPartitions partitions to copy
   * @return the job to copy all of the specified partitions
   *
   * @throws StateUpdateException if there's an error writing to the DB
   */
  public ReplicationJob createJobForCopyDynamicPartitions(
      long auditLogId,
      long auditLogEntryCreateTime,
      List<NamedPartition> namedPartitions) throws StateUpdateException {

    ReplicationOperation replicationOperation = ReplicationOperation.COPY_PARTITIONS;

    List<Partition> partitions = NamedPartition.toPartitions(namedPartitions);
    List<String> partitionNames = NamedPartition.toNames(namedPartitions);

    // The common location is the common path that all the partitions share.
    Optional<Path> commonLocation =
        ReplicationUtils.getCommonDirectory(ReplicationUtils.getLocations(partitions));

    Partition samplePartition = namedPartitions.get(0).getPartition();
    HiveObjectSpec tableSpec =
        new HiveObjectSpec(samplePartition.getDbName(), samplePartition.getTableName());

    Map<String, String> extras = new HashMap<>();
    extras.put(PersistedJobInfo.AUDIT_LOG_ID_EXTRAS_KEY, Long.toString(auditLogId));
    extras.put(PersistedJobInfo.AUDIT_LOG_ENTRY_CREATE_TIME_KEY,
        Long.toString(auditLogEntryCreateTime));

    PersistedJobInfo persistedJobInfo =
        PersistedJobInfo.createDeferred(
            replicationOperation, ReplicationStatus.PENDING, commonLocation,
            srcCluster.getName(), tableSpec, partitionNames,
            Optional.empty(), Optional.empty(), Optional.empty(), extras);

    ReplicationTask replicationTask = new CopyPartitionsTask(conf, destinationObjectFactory,
        objectConflictHandler, srcCluster, destCluster, tableSpec, partitionNames, commonLocation,
        copyPartitionJobExecutor, directoryCopier);

    return new ReplicationJob(conf, replicationTask, onStateChangeHandler, persistedJobInfo);
  }

  /**
   * Create a mapping from a Hive object specification to the Thrift Hive Table object.
   *
   * @param tables tables to include in the map
   * @return a map from the Hive object specification to the Thrift Hive Table object
   */
  private Map<HiveObjectSpec, Table> createTableLookupMap(List<Table> tables) {
    // Create a map from the table spec to the table object. We'll need this
    // for getting the table that a partition belongs to
    Map<HiveObjectSpec, Table> specToTable = new HashMap<>();
    for (Table table : tables) {
      HiveObjectSpec spec = new HiveObjectSpec(table);
      specToTable.put(spec, table);
    }
    return specToTable;
  }

  /**
   * Create a replication job to drop a table.
   *
   * @param auditLogId ID of the audit log entry where this table was dropped
   * @param auditLogEntryCreateTime when the audit log entry was created
   * @param table the table to drop
   * @return the job to drop the table
   *
   * @throws StateUpdateException if there's an error writing to the DB
   */
  public ReplicationJob createJobForDropTable(
      long auditLogId,
      long auditLogEntryCreateTime,
      Table table) throws StateUpdateException {
    ReplicationOperation replicationOperation = ReplicationOperation.DROP_TABLE;

    Map<String, String> extras = new HashMap<>();
    extras.put(PersistedJobInfo.AUDIT_LOG_ID_EXTRAS_KEY, Long.toString(auditLogId));
    extras.put(PersistedJobInfo.AUDIT_LOG_ENTRY_CREATE_TIME_KEY,
        Long.toString(auditLogEntryCreateTime));

    HiveObjectSpec tableSpec = new HiveObjectSpec(table);

    PersistedJobInfo persistedJobInfo =
        PersistedJobInfo.createDeferred(
            replicationOperation, ReplicationStatus.PENDING,
            ReplicationUtils.getLocation(table), srcCluster.getName(), tableSpec,
            Collections.emptyList(), ReplicationUtils.getTldt(table), Optional.empty(),
            Optional.empty(), extras);

    return new ReplicationJob(
        conf,
        new DropTableTask(
            srcCluster,
            destCluster,
            tableSpec,
            ReplicationUtils.getTldt(table)),
        onStateChangeHandler,
        persistedJobInfo);
  }

  /**
   * Create a job to drop a partition.
   *
   * @param auditLogId ID of the audit log entry where this partition was dropped
   * @param auditLogEntryCreateTime when the audit log entry was created
   * @param namedPartition the partition to drop
   * @return the job to drop the partition
   *
   * @throws StateUpdateException if there is an error writing to the DB
   */
  public ReplicationJob createJobForDropPartition(
      long auditLogId,
      long auditLogEntryCreateTime,
      NamedPartition namedPartition) throws StateUpdateException {
    Map<String, String> extras = new HashMap<>();
    extras.put(PersistedJobInfo.AUDIT_LOG_ID_EXTRAS_KEY, Long.toString(auditLogId));
    extras.put(PersistedJobInfo.AUDIT_LOG_ENTRY_CREATE_TIME_KEY,
        Long.toString(auditLogEntryCreateTime));

    ReplicationOperation replicationOperation = ReplicationOperation.DROP_PARTITION;
    HiveObjectSpec partitionSpec = new HiveObjectSpec(namedPartition);
    List<String> partitionNames = new ArrayList<>();
    partitionNames.add(namedPartition.getName());
    Optional<String> partitionTldt = ReplicationUtils.getTldt(namedPartition.getPartition());
    PersistedJobInfo persistedJobInfo =
        PersistedJobInfo.createDeferred(
            replicationOperation, ReplicationStatus.PENDING,
            ReplicationUtils.getLocation(namedPartition.getPartition()),
            srcCluster.getName(), partitionSpec.getTableSpec(), partitionNames, partitionTldt,
            Optional.empty(), Optional.empty(), extras);

    return new ReplicationJob(
        conf,
        new DropPartitionTask(
            srcCluster,
            destCluster,
            partitionSpec,
            partitionTldt),
        onStateChangeHandler,
        persistedJobInfo);
  }

  /**
   * Create a job to rename a table.
   *
   * @param auditLogId ID of the audit log entry where this partition was dropped
   * @param auditLogEntryCreateTime when the audit log entry was created
   * @param renameFromTable the table to rename from
   * @param renameToTable the table to rename to
   * @return the job to rename the specified table
   *
   * @throws StateUpdateException if there's an error writing to the DB
   */
  public ReplicationJob createJobForRenameTable(
      long auditLogId,
      long auditLogEntryCreateTime,
      Table renameFromTable,
      Table renameToTable) throws StateUpdateException {
    ReplicationOperation replicationOperation = ReplicationOperation.RENAME_TABLE;

    Map<String, String> extras = new HashMap<>();
    extras.put(PersistedJobInfo.AUDIT_LOG_ID_EXTRAS_KEY, Long.toString(auditLogId));
    extras.put(PersistedJobInfo.AUDIT_LOG_ENTRY_CREATE_TIME_KEY,
        Long.toString(auditLogEntryCreateTime));

    HiveObjectSpec renameFromTableSpec = new HiveObjectSpec(renameFromTable);
    HiveObjectSpec renameToTableSpec = new HiveObjectSpec(renameToTable);
    Optional<Path> renameFromPath = ReplicationUtils.getLocation(renameFromTable);
    Optional<Path> renameToPath = ReplicationUtils.getLocation(renameToTable);

    PersistedJobInfo persistedJobInfo =
        PersistedJobInfo.createDeferred(
            replicationOperation, ReplicationStatus.PENDING, renameFromPath,
            srcCluster.getName(), renameFromTableSpec,
            new ArrayList<>(), ReplicationUtils.getTldt(renameFromTable),
            Optional.of(renameToTableSpec), renameToPath, extras);

    return new ReplicationJob(
        conf,
        new RenameTableTask(conf,
            srcCluster,
            destCluster,
            destinationObjectFactory,
            objectConflictHandler,
            renameFromTableSpec,
            renameToTableSpec,
            renameFromPath,
            renameToPath,
            ReplicationUtils.getTldt(renameFromTable),
            copyPartitionJobExecutor,
            directoryCopier),
        onStateChangeHandler,
        persistedJobInfo);
  }

  /**
   * Create a job to rename a partition.
   *
   * @param auditLogId ID of the audit log entry where this partition was dropped
   * @param auditLogEntryCreateTime when the audit log entry was created
   * @param renameFromPartition partition to rename from
   * @param renameToPartition partition to rename to
   * @return a job to rename the partition
   *
   * @throws StateUpdateException if there's an error writing to the DB
   */
  public ReplicationJob createJobForRenamePartition(
      long auditLogId,
      long auditLogEntryCreateTime,
      NamedPartition renameFromPartition,
      NamedPartition renameToPartition) throws StateUpdateException {
    ReplicationOperation replicationOperation = ReplicationOperation.RENAME_PARTITION;

    Map<String, String> extras = new HashMap<>();
    extras.put(PersistedJobInfo.AUDIT_LOG_ID_EXTRAS_KEY, Long.toString(auditLogId));
    extras.put(PersistedJobInfo.AUDIT_LOG_ENTRY_CREATE_TIME_KEY,
        Long.toString(auditLogEntryCreateTime));

    HiveObjectSpec renameFromPartitionSpec = new HiveObjectSpec(renameFromPartition);
    HiveObjectSpec renameToPartitionSpec = new HiveObjectSpec(renameToPartition);
    Optional renameFromPath = ReplicationUtils.getLocation(renameFromPartition.getPartition());
    Optional renameToPath = ReplicationUtils.getLocation(renameToPartition.getPartition());

    PersistedJobInfo persistedJobInfo =
        PersistedJobInfo.createDeferred(
            replicationOperation, ReplicationStatus.PENDING, renameFromPath,
            srcCluster.getName(), renameFromPartitionSpec,
            new ArrayList<>(), ReplicationUtils.getTldt(renameFromPartition.getPartition()),
            Optional.of(renameToPartitionSpec), renameToPath, extras);

    return new ReplicationJob(
        conf,
        new RenamePartitionTask(
            conf,
            destinationObjectFactory,
            objectConflictHandler,
            srcCluster,
            destCluster,
            renameFromPartitionSpec,
            renameToPartitionSpec,
            renameFromPath,
            renameToPath,
            ReplicationUtils.getTldt(renameFromPartition.getPartition()), directoryCopier),
        onStateChangeHandler,
        persistedJobInfo);
  }

  private enum OperationType {
    COPY, DROP, RENAME
  }

  /**
   * Creates ReplicationJobs for a list of AuditLogEntries. The lists of lists
   * return correspond naturally (ie first AuditLogEntry corresponds to first List).
   * The filters are used in each request
   * @param auditLogEntries A list of N AuditLogEntries
   * @param replicationFilters A list of M replication filter to be used in each AuditLogEntry
   * @return A List of List of Replication Jobs, with each List of Replication Job corresponding
   *     to the i^th audit log entry
   * @throws StateUpdateException if there is a MySQL issue
   */
  public List<List<ReplicationJob>> createReplicationJobs(
      List<AuditLogEntry> auditLogEntries,
      List<ReplicationFilter> replicationFilters) throws StateUpdateException {
    List<List<ReplicationJob>> replicationJobs = new ArrayList<>();
    List<PersistedJobInfo> toPersist = new ArrayList<>();
    for (AuditLogEntry auditLogEntry : auditLogEntries) {
      List<ReplicationJob> replicationJobs1 =
          createReplicationJobsSingle(auditLogEntry, replicationFilters);
      replicationJobs.add(replicationJobs1);
      for (ReplicationJob replicationJob: replicationJobs1) {
        toPersist.add(replicationJob.getPersistedJobInfo());
      }
    }
    persistedJobInfoStore.createMany(toPersist);
    return replicationJobs;
  }
  /**
   * Converts the audit log entry into a set of replication jobs that have the persisted elements
   * properly set.
   *
   * @param auditLogEntry the audit log entry associated with the actions that need to be replicated

   * @throws StateUpdateException if there's an error writing to the DB
   */
  private List<ReplicationJob> createReplicationJobsSingle(
      AuditLogEntry auditLogEntry,
      List<ReplicationFilter> replicationFilters) throws StateUpdateException {
    List<ReplicationJob> replicationJobs = new ArrayList<>();

    for (ReplicationFilter replicationFilter : replicationFilters) {
      if (!replicationFilter.accept(auditLogEntry)) {
        LOG.debug(String.format("Audit log entry id: %s filtered out by %s", auditLogEntry,
            replicationFilter.getClass().getSimpleName()));
        return replicationJobs;
      }
    }


    // TODO: Rewrite once HIVE-12865 is resolved.
    // The inputs and outputs for exchange partitions in the audit log is
    // broken as per HIVE-12865. This workaround is to parse the exchange
    // partition command to figure out what the input and output partitions
    // are. auditLogEntry.getCommandType() may be null for the unit test only.
    if (auditLogEntry.getOutputTables().size() == 0
        && (auditLogEntry.getCommandType() == null
        || auditLogEntry.getCommandType() == HiveOperation.ALTERTABLE_EXCHANGEPARTITION)) {
      // This is probably an exchange partition command
      ExchangePartitionParser parser = new ExchangePartitionParser();
      boolean parsed = parser.parse(auditLogEntry.getCommand());
      if (parsed) {
        LOG.debug(
            String.format("Parsed audit log id: %s " + "query: %s as an exchange partition query",
                auditLogEntry.getId(), auditLogEntry.getCommand()));
        // Since we're missing the modified time for the source
        // partition, just copy for now

        HiveObjectSpec exchangeToSpec = new HiveObjectSpec(parser.getExchangeToSpec().getDbName(),
            parser.getExchangeToSpec().getTableName(), parser.getPartitionName());

        Table exchangeToTable = new Table();
        exchangeToTable.setDbName(exchangeToSpec.getDbName());
        exchangeToTable.setTableName(exchangeToSpec.getTableName());

        Partition exchangeToPartition = new Partition();
        exchangeToPartition.setDbName(exchangeToSpec.getDbName());
        exchangeToPartition.setTableName(exchangeToSpec.getTableName());
        exchangeToPartition.setValues(parser.getPartitionValues());

        for (ReplicationFilter replicationFilter : replicationFilters) {
          if (!replicationFilter.accept(exchangeToTable,
              new NamedPartition(exchangeToSpec.getPartitionName(), exchangeToPartition))) {
            LOG.debug(
                String.format("Exchange partition from audit log" + " id: %s filtered out by %s",
                    auditLogEntry.getId(), replicationFilter.getClass().getSimpleName()));
            return replicationJobs;
          }
        }
        ReplicationJob job = createJobForCopyPartition(auditLogEntry.getId(),
            auditLogEntry.getCreateTime().getTime(), exchangeToSpec);

        replicationJobs.add(job);
        return replicationJobs;
      } else {
        LOG.warn("Error parsing query " + auditLogEntry.getCommand());
      }
    }
    // End exchange partitions workaround

    // Filter out CLI commands that don't have any outputs. This logic will need to be revisited
    // when the definition of inputs / outputs is revised for drop operations.
    if (!HiveOperation.isThriftOperation(auditLogEntry.getCommandType())
        && auditLogEntry.getOutputTables().size() == 0
        && auditLogEntry.getOutputPartitions().size() == 0) {
      LOG.debug(String.format(
          "Audit log entry id: %s filtered out " + "since it has no output tables or partitions",
          auditLogEntry.getId()));
      return replicationJobs;
    }

    OperationType operationType = null;
    switch (auditLogEntry.getCommandType()) {
      case DROPTABLE:
      case THRIFT_DROP_TABLE:
      case DROPVIEW:
      case ALTERTABLE_DROPPARTS:
      case THRIFT_DROP_PARTITION:
        operationType = OperationType.DROP;
        break;
      case ALTERTABLE_RENAME:
      case ALTERVIEW_RENAME:
      case ALTERTABLE_RENAMEPART:
        operationType = OperationType.RENAME;
        break;
      case THRIFT_ALTER_TABLE:
        String inputTableName = auditLogEntry.getInputTable().getTableName();
        if (auditLogEntry.getOutputTables().size() == 1
            && !auditLogEntry.getOutputTables().get(0).getTableName().equals(inputTableName)) {
          operationType = OperationType.RENAME;
        } else {
          operationType = OperationType.COPY;
        }
        break;
      case THRIFT_ALTER_PARTITION:
        NamedPartition inputPartition = auditLogEntry.getInputPartition();
        List<NamedPartition> outputPartitions = auditLogEntry.getOutputPartitions();
        if (inputPartition != null && outputPartitions.size() == 1
            && !inputPartition.getName().equals(outputPartitions.get(0).getName())) {
          operationType = OperationType.RENAME;
        } else {
          operationType = OperationType.COPY;
        }
        break;
      default:
        operationType = OperationType.COPY;
    }

    List<Table> outputTables = new ArrayList<>(auditLogEntry.getOutputTables());
    List<NamedPartition> outputPartitions = new ArrayList<>(auditLogEntry.getOutputPartitions());
    List<Table> referenceTables = auditLogEntry.getReferenceTables();

    // Look at inputs as Thrift drop operations have that in the inputs
    List<Table> inputTables = new ArrayList<>();
    if (auditLogEntry.getInputTable() != null) {
      inputTables.add(auditLogEntry.getInputTable());
    }
    List<NamedPartition> inputPartitions = new ArrayList<>();
    if (auditLogEntry.getInputPartition() != null) {
      inputPartitions.add(auditLogEntry.getInputPartition());
    }

    // Filter out tables and partitions that we may not want to replicate
    filterObjects(replicationFilters, outputTables, outputPartitions,
        createTableLookupMap(referenceTables));
    filterObjects(replicationFilters, inputTables, inputPartitions,
        createTableLookupMap(referenceTables));

    switch (operationType) {
      case COPY:
        // Handle the tables. The table is present in add partition
        // calls, so skip in those cases. Also, for load commands, the table is mentioned as well
        // in case of a partition load, so that can be omitted.
        boolean shouldNotAddTables =
            auditLogEntry.getCommandType() == HiveOperation.ALTERTABLE_ADDPARTS
                || (auditLogEntry.getCommandType() == HiveOperation.LOAD
                && auditLogEntry.getOutputPartitions().size() > 0);
        if (!shouldNotAddTables) {
          for (Table t : outputTables) {
            replicationJobs.add(createJobForCopyTable(auditLogEntry.getId(),
                auditLogEntry.getCreateTime().getTime(), t));
          }
        }

        // Handle the partitions
        // See if this is a dynamic partition insert
        if (auditLogEntry.getOutputPartitions().size() > 1
            && ReplicationUtils.fromSameTable(NamedPartition.toPartitions(outputPartitions))) {
          replicationJobs.add(createJobForCopyDynamicPartitions(auditLogEntry.getId(),
              auditLogEntry.getCreateTime().getTime(), auditLogEntry.getOutputPartitions()));
        } else {
          // Otherwise create separate insert partition jobs for each
          // partition
          for (NamedPartition p : outputPartitions) {
            replicationJobs.add(createJobForCopyPartition(auditLogEntry.getId(),
                auditLogEntry.getCreateTime().getTime(), p));
          }
        }
        break;
      case DROP:
        for (Table t : outputTables) {
          replicationJobs.add(createJobForDropTable(auditLogEntry.getId(),
              auditLogEntry.getCreateTime().getTime(), t));
        }
        for (NamedPartition p : outputPartitions) {
          replicationJobs.add(createJobForDropPartition(auditLogEntry.getId(),
              auditLogEntry.getCreateTime().getTime(), p));
        }
        // Thrift operations have the dropped object in the inputs
        if (HiveOperation.isThriftOperation(auditLogEntry.getCommandType())) {
          for (Table t : inputTables) {
            replicationJobs.add(createJobForDropTable(auditLogEntry.getId(),
                auditLogEntry.getCreateTime().getTime(), t));
          }
          for (NamedPartition p : inputPartitions) {
            replicationJobs.add(createJobForDropPartition(auditLogEntry.getId(),
                auditLogEntry.getCreateTime().getTime(), p));
          }
        }
        break;
      case RENAME:
        // There's an edge case to consider - let's say table A is
        // renamed to table B, however, table A is excluded by the
        // user specified filter. In this case, we still do the rename.
        if (outputTables.size() == 0 && outputPartitions.size() == 0) {
          // This means that the table was filtered out
        } else if (auditLogEntry.getInputTable() != null) {
          // Handle a rename table
          replicationJobs.add(createJobForRenameTable(auditLogEntry.getId(),
              auditLogEntry.getCreateTime().getTime(), auditLogEntry.getInputTable(),
              auditLogEntry.getOutputTables().get(0)));
        } else if (auditLogEntry.getInputPartition() != null) {
          // Handle a rename partition
          replicationJobs.add(createJobForRenamePartition(auditLogEntry.getId(),
              auditLogEntry.getCreateTime().getTime(), auditLogEntry.getInputPartition(),
              auditLogEntry.getOutputPartitions().get(0)));
        } else {
          throw new RuntimeException("Shouldn't happen!");
        }
        break;
      default:
        throw new RuntimeException("Operation not handled: " + operationType);
    }

    LOG.debug("Converted audit log entry " + auditLogEntry + " to " + replicationJobs);

    return replicationJobs;
  }

  /**
   * Based on the supplied filter, remove tables and partitions that should not be replicated.
   *
   * @param filters the filters to remove undesired objects
   * @param tables the tables to filter
   * @param partitions the partitions to filter
   */
  private void filterObjects(
      List<ReplicationFilter> filters,
      List<Table> tables,
      List<NamedPartition> partitions,
      Map<HiveObjectSpec, Table> tableLookupMap) {

    // Create the list of tables that the partitions belong to. These
    // tables were included by the hook, but don't need to be replicated,
    // but is needed for running the filter.
    Set<HiveObjectSpec> tablesToNotReplicate = new HashSet<>();
    for (NamedPartition pwn : partitions) {
      Partition partition = pwn.getPartition();
      HiveObjectSpec tableSpec = new HiveObjectSpec(
          partition.getDbName(), partition.getTableName());
      tablesToNotReplicate.add(tableSpec);
    }

    // Remove all the partitions that don't match the filter
    Iterator<NamedPartition> partitionIterator = partitions.iterator();
    while (partitionIterator.hasNext()) {
      NamedPartition pwn = partitionIterator.next();
      Partition partition = pwn.getPartition();
      HiveObjectSpec partitionSpec = new HiveObjectSpec(pwn);

      Table table =
          tableLookupMap.get(new HiveObjectSpec(partition.getDbName(), partition.getTableName()));
      for (ReplicationFilter filter : filters) {
        if (!filter.accept(table, pwn)) {
          LOG.debug(
              String.format("%s filtering out: %s", filter.getClass().getName(), partitionSpec));
          partitionIterator.remove();
          break;
        }
      }
    }

    // Remove all tables that don't pass the filter, or don't need to be
    // replicated
    Iterator<Table> tableIterator = tables.iterator();
    while (tableIterator.hasNext()) {
      Table table = tableIterator.next();
      HiveObjectSpec tableSpec = new HiveObjectSpec(table);
      for (ReplicationFilter filter : filters) {
        if (!filter.accept(table)) {
          LOG.debug(String.format("%s filtering out: %s", filter.getClass().getName(), tableSpec));
          tableIterator.remove();
          break;
        }
      }
    }
  }
}
