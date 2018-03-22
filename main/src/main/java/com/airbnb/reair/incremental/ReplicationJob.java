package com.airbnb.reair.incremental;

import com.airbnb.reair.common.DistCpException;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.incremental.configuration.ConfigurationException;
import com.airbnb.reair.incremental.db.PersistedJobInfo;
import com.airbnb.reair.incremental.deploy.ConfigurationKeys;
import com.airbnb.reair.incremental.primitives.ReplicationTask;
import com.airbnb.reair.multiprocessing.Job;
import com.airbnb.reair.multiprocessing.LockSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * A job that performs a replication task and can be executed in parallel though the
 * ParallelJobExecutor.
 */
public class ReplicationJob extends Job {
  private static final Log LOG = LogFactory.getLog(ReplicationJob.class);
  // Default number of times to retry a job if it fails.
  public static final int DEFAULT_JOB_RETRIES = 8;

  private Configuration conf;
  private ReplicationTask replicationTask;
  private OnStateChangeHandler onStateChangeHandler;
  private PersistedJobInfo persistedJobInfo;

  /**
   * Constructor for a replication job that can be run in the ParallelJobExecutor.
   *
   * @param replicationTask the task that this job should run
   * @param onStateChangeHandler The handler to run when the state of this job changes. E.g. start
   *                             or finish.
   * @param persistedJobInfo the PersistedJobInfo that should be associated with this job
   */
  public ReplicationJob(
      Configuration conf,
      ReplicationTask replicationTask,
      OnStateChangeHandler onStateChangeHandler,
      PersistedJobInfo persistedJobInfo) {
    this.conf = conf;
    this.replicationTask = replicationTask;
    this.onStateChangeHandler = onStateChangeHandler;
    this.persistedJobInfo = persistedJobInfo;
  }

  public PersistedJobInfo getPersistedJobInfo() {
    return persistedJobInfo;
  }

  @Override
  public int run() {
    int maxAttempts = 1 + Math.max(0, conf.getInt(ConfigurationKeys.JOB_RETRIES, 8));
    for (int attempt = 0; attempt < maxAttempts; attempt++) {
      try {
        onStateChangeHandler.onStart(this);
        RunInfo runInfo = replicationTask.runTask();
        LOG.info(String.format("Replication job id: %s finished " + "with status %s",
            persistedJobInfo.getId(), runInfo.getRunStatus()));
        onStateChangeHandler.onComplete(runInfo, this);

        switch (runInfo.getRunStatus()) {
          case SUCCESSFUL:
          case NOT_COMPLETABLE:
            return 0;
          case FAILED:
            return -1;
          default:
            throw new RuntimeException("State not handled: " + runInfo.getRunStatus());
        }
      } catch (HiveMetastoreException | IOException | DistCpException e) {
        LOG.error("Got an exception!", e);
      } catch (StateUpdateException | ConfigurationException e) {
        // Indicates an error with the system - fail the job.
        LOG.error("Got an exception!", e);
        return -1;
      }

      if (attempt == maxAttempts - 1) {
        break;
      }

      LOG.error("Because job id: " + getId() + " was not successful, "
          + "it will be retried after sleeping.");
      try {
        ReplicationUtils.exponentialSleep(attempt);
      } catch (InterruptedException e) {
        LOG.warn("Got interrupted", e);
        return -1;
      }
      attempt++;
    }
    return -1;
  }

  @Override
  public LockSet getRequiredLocks() {
    return replicationTask.getRequiredLocks();
  }

  @Override
  public String toString() {
    return "ReplicationJob{" + "persistedJobInfo=" + persistedJobInfo + "}";
  }

  /**
   * Get the ID associated with this job.
   *
   * @return this job's ID
   */
  public long getId() {
    return persistedJobInfo.getId();
  }

  /**
   * Get the time when this job was created.
   *
   * @return the create time
   */
  public long getCreateTime() {
    Optional<String> createTime = Optional.ofNullable(
        getPersistedJobInfo().getExtras().get(PersistedJobInfo.AUDIT_LOG_ENTRY_CREATE_TIME_KEY));

    return createTime.map(Long::parseLong).orElse(Long.valueOf(0));
  }

  /**
   * Get the ID's for the jobs that this job is waiting on.
   *
   * @return a list of job ID's that this job is waiting on
   */
  public Collection<Long> getParentJobIds() {
    Set<Job> parentJobs = getParentJobs();
    List<Long> parentJobIds = new ArrayList<>();

    for (Job parentJob : parentJobs) {
      // Generally good to avoid casting, but done here since the getId()
      // is not a part of the Job class.
      ReplicationJob replicationJob = (ReplicationJob) parentJob;
      parentJobIds.add(replicationJob.getId());
    }

    return parentJobIds;
  }
}
