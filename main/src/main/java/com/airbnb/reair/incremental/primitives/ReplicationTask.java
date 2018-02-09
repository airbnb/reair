package com.airbnb.reair.incremental.primitives;

import com.airbnb.reair.common.DistCpException;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.incremental.RunInfo;
import com.airbnb.reair.incremental.configuration.ConfigurationException;
import com.airbnb.reair.multiprocessing.LockSet;

import java.io.IOException;

/**
 * Interface for a replication task. A replication task is one of many primitives that can be used
 * to replicate data and actions from the source warehouse to the destination warehouse. Generally,
 * a ReplicationTask is executed by a ReplicationJob.
 */
public interface ReplicationTask {
  /**
   * Runs the replication task without retries.
   *
   * @return RunInfo containing how the task execution went
   * @throws HiveMetastoreException if there is an error making a metastore call
   * @throws IOException if there is an error with writing to files
   * @throws DistCpException if there is an error running DistCp
   * @throws ConfigurationException if the config is improper
   */
  RunInfo runTask()
      throws ConfigurationException, HiveMetastoreException, IOException, DistCpException;

  /**
   * To handle concurrency issues, replication tasks should specify a set of locks so that two
   * conflicting replication tasks do not run at the same time.
   *
   * @return a set of locks that this task should acquire before running
   */
  LockSet getRequiredLocks();
}
