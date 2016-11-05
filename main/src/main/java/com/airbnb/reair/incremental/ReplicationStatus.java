package com.airbnb.reair.incremental;

/**
 * States that a replication job can be in.
 */
public enum ReplicationStatus {
  // Created, but not yet running.
  PENDING,
  // Executing operations.
  RUNNING,
  // Finished running with a success.
  SUCCESSFUL,
  // Finished running but with a failure.
  FAILED,
  // A job that is not possible to complete. For example, trying to copy a non-existent table.
  NOT_COMPLETABLE,
  // A job that was aborted and should not be run again.
  ABORTED,
}
