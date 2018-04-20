package com.airbnb.reair.incremental.auditlog;

public class MetricNames {
  public static final String TASK_BY_STATUS_COUNT = "task.%s";
  public static final String REPLICATION_JOBS_AGE_COUNT = "replication_jobs.age.%ds";
  public static final String JOBS_IN_MEMORY_GAUGE = "jobs_in_memory";
  public static final String AUDIT_LOG_ENTRIES_COUNT = "audit_log_entries_read";
  public static final String PERSISTED_JOBS_COUNT = "replication_jobs_created";
}
