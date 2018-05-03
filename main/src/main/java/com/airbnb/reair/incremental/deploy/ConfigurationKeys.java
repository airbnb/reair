package com.airbnb.reair.incremental.deploy;

/**
 * Keys used in the configuration for deploying the replication server.
 */
public class ConfigurationKeys {
  // JDBC URL to the DB containing the audit log table
  public static final String AUDIT_LOG_JDBC_URL = "airbnb.reair.audit_log.db.jdbc_url";
  // User for the audit log DB
  public static final String AUDIT_LOG_DB_USER = "airbnb.reair.audit_log.db.username";
  // Password for the audit log DB
  public static final String AUDIT_LOG_DB_PASSWORD = "airbnb.reair.audit_log.db.password";
  // Name of the audit log table
  public static final String AUDIT_LOG_DB_TABLE = "airbnb.reair.audit_log.db.table_name";
  // Name of the table containing serialized thrift objects from the audit log
  public static final String AUDIT_LOG_OBJECTS_DB_TABLE =
      "airbnb.reair.audit_log.objects.db.table_name";
  // Name of the table containing mapred job stats
  public static final String AUDIT_LOG_MAPRED_STATS_DB_TABLE =
      "airbnb.reair.audit_log.mapred_stats.db.table_name";
  // Affects how many AuditLogEntries are read and processed at once, default 128
  public static final String AUDIT_LOG_PROCESSING_BATCH_SIZE =
      "airbnb.reair.audit_log.batch_size";

  // JDB URL to the DB containing the replication state tables
  public static final String STATE_JDBC_URL = "airbnb.reair.state.db.jdbc_url";
  // User for the state DB
  public static final String STATE_DB_USER = "airbnb.reair.state.db.username";
  // Password for the state DB
  public static final String STATE_DB_PASSWORD = "airbnb.reair.state.db.password";
  // Name of the table containing replication job state
  public static final String STATE_DB_TABLE = "airbnb.reair.state.db.table_name";
  // Name of the table containing key/value pairs
  public static final String STATE_KV_DB_TABLE = "airbnb.reair.state.kv.db.table_name";

  // When running queries to the DB, the number of times to retry if there's an error
  public static final String DB_QUERY_RETRIES =
      "airbnb.reair.db.query.retries";

  // monitoring via statsd settings
  public static final String STATSD_ENABLED = "airbnb.reair.statsd.enabled";
  // default: localhost
  public static final String STATSD_HOST = "airbnb.reair.statsd.host";
  // default: 8125
  public static final String STATSD_PORT = "airbnb.reair.statsd.port";
  // default: reair
  public static final String STATSD_PREFIX = "airbnb.reair.statsd.prefix";
  // ReplicationJob delay reported thresholds (seconds) (comma-separated)
  public static final String REPLICATION_JOB_METRIC_THRESHOLDS =
      "airbnb.reair.replication_job.threshold_seconds";
  // Frequency at which to report stats in the ReplicationJobRegistry
  public static final String REPLICATION_JOB_REGISTRY_REPORT_INTERVAL_SEC =
      "airbnb.reair.replication.report.threshold_seconds";

  // Name to use for the source cluster
  public static final String SRC_CLUSTER_NAME = "airbnb.reair.clusters.src.name";
  // URL to the source cluster's metastore Thrift server.
  public static final String SRC_CLUSTER_METASTORE_URL = "airbnb.reair.clusters.src.metastore.url";
  // The root of the HDFS directory for the source cluster
  public static final String SRC_HDFS_ROOT = "airbnb.reair.clusters.src.hdfs.root";
  // The root of the temporary directory for storing temporary files on the source cluster
  public static final String SRC_HDFS_TMP = "airbnb.reair.clusters.src.hdfs.tmp";

  // Name to use for the destination cluster
  public static final String DEST_CLUSTER_NAME = "airbnb.reair.clusters.dest.name";
  // URL to the destination cluster's metastore Thrift server.
  public static final String DEST_CLUSTER_METASTORE_URL =
      "airbnb.reair.clusters.dest.metastore.url";
  // The root of the HDFS directory for the destination cluster
  public static final String DEST_HDFS_ROOT = "airbnb.reair.clusters.dest.hdfs.root";
  // The root of the temporary directory for storing temporary files on the destination cluster
  public static final String DEST_HDFS_TMP = "airbnb.reair.clusters.dest.hdfs.tmp";

  // Class to use for filtering out entries from the audit log
  public static final String OBJECT_FILTER_CLASS = "airbnb.reair.object.filter";
  // Number of threads to use for copying objects in the incremental replication server
  public static final String WORKER_THREADS = "airbnb.reair.worker.threads";
  // Maximum number of jobs to keep in memory in the incremental replication server
  public static final String MAX_JOBS_IN_MEMORY = "airbnb.reair.jobs.in_memory_count";
  // The port for the Thrift server to listen on
  public static final String THRIFT_SERVER_PORT = "airbnb.reair.thrift.port";
  // When copying tables or partitions using an MR job, fail the job and retry if the job takes
  // longer than this many seconds.
  public static final String COPY_JOB_TIMEOUT_SECONDS = "airbnb.reair.copy.timeout.seconds";
  // Whether to use a size based timeout for copy jobs
  public static final String COPY_JOB_DYNAMIC_TIMEOUT_ENABLED =
      "airbnb.reair.copy.timeout.dynamic.enabled";
  public static final String COPY_JOB_DYNAMIC_TIMEOUT_MS_PER_GB_PER_MAPPER =
      "airbnb.reair.copy.timeout.dynamic.ms_per_gb_per_mapper";
  public static final String COPY_JOB_DYNAMIC_TIMEOUT_BASE =
      "airbnb.reair.copy.timeout.dynamic.base.ms";
  public static final String COPY_JOB_DYNAMIC_TIMEOUT_MAX =
      "airbnb.reair.copy.timeout.dynamic.max.ms";
  // If a replication job fails, the number of times to retry the job.
  public static final String JOB_RETRIES = "airbnb.reair.job.retries";
  // After a copy, whether to set / check that modified times for the copied files match between
  // the source and the destination. Set to false for file systems that don't support changes
  // to the modified time.
  public static final String SYNC_MODIFIED_TIMES_FOR_FILE_COPY =
      "airbnb.reair.copy.sync_modified_times";

  // Following are settings pertinent to batch replication only.

  // Location to store the output of batch replication jobs
  public static final String BATCH_JOB_OUTPUT_DIR = "airbnb.reair.clusters.batch.output.dir";
  // Location to store the input for replication jobs
  public static final String BATCH_JOB_INPUT_LIST = "airbnb.reair.clusters.batch.input";
  // A list of regex'es to use to exclude tables in batch replication
  public static final String BATCH_JOB_METASTORE_BLACKLIST =
      "airbnb.reair.clusters.batch.metastore.blacklist";
  // Name of the class for creating the cluster object in batch replication. Mainly for testing.
  public static final String BATCH_JOB_CLUSTER_FACTORY_CLASS =
      "airbnb.reair.clusters.batch.cluster.factory.class";
  // Whether to overwrite newer tables/partitions on the destination. Default is true.
  public static final String BATCH_JOB_OVERWRITE_NEWER =
      "airbnb.reair.batch.overwrite.newer";
  // The number of reducers to use for jobs where reducers perform metastore operations
  public static final String BATCH_JOB_METASTORE_PARALLELISM =
          "airbnb.reair.batch.metastore.parallelism";
  // The number of reducers to use for jobs where reducers perform file copies
  public static final String BATCH_JOB_COPY_PARALLELISM =
      "airbnb.reair.batch.copy.parallelism";
  // Whether to try to compare checksums to validate file copies when possible
  public static final String BATCH_JOB_VERIFY_COPY_CHECKSUM =
      "airbnb.reair.batch.copy.checksum.verify";
}
