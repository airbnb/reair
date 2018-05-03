package com.airbnb.reair.incremental.auditlog;

/**
 * Contains string names of metrics that are reported to statsd.
 */
public class MetricNames {
  // Count of tasks by status in ReplicationCounters
  public static final String REPLICATION_STATUS_COUNT = "replication.%s";
  // Count of jobs above age thresholds in ReplicationJobRegistry
  public static final String REPLICATION_JOBS_AGE_COUNT = "replication_jobs.age.%ds";
  // Counts how many jobs are in memory
  public static final String JOBS_IN_MEMORY_GAUGE = "jobs_in_memory";
  // Counts the number of audit log entries read
  public static final String AUDIT_LOG_ENTRIES_COUNT = "audit_log_entries_read";
  // Counts the number of jobs persisted to the database
  public static final String PERSISTED_JOBS_COUNT = "replication_jobs_created";
}
