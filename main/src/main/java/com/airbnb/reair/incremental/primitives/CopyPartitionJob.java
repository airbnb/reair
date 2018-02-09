package com.airbnb.reair.incremental.primitives;

import com.airbnb.reair.common.DistCpException;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.incremental.ReplicationUtils;
import com.airbnb.reair.incremental.RunInfo;
import com.airbnb.reair.incremental.configuration.ConfigurationException;
import com.airbnb.reair.multiprocessing.Job;
import com.airbnb.reair.multiprocessing.LockSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

public class CopyPartitionJob extends Job {

  private static final Log LOG = LogFactory.getLog(CopyPartitionJob.class);

  private CopyPartitionTask copyPartitionTask;
  private CopyPartitionsCounter copyPartitionsCounter;

  public CopyPartitionJob(
      CopyPartitionTask copyPartitionTask,
      CopyPartitionsCounter copyPartitionsCounter) {
    this.copyPartitionTask = copyPartitionTask;
    this.copyPartitionsCounter = copyPartitionsCounter;
  }

  @Override
  public int run() {

    int attempt = 0;
    while (true) {
      try {
        RunInfo runInfo = copyPartitionTask.runTask();
        LOG.debug(String.format("Copy partition task %s finished " + "with status %s",
            copyPartitionTask.getSpec(), runInfo.getRunStatus()));

        switch (runInfo.getRunStatus()) {
          case SUCCESSFUL:
          case NOT_COMPLETABLE:
            copyPartitionsCounter.incrementBytesCopied(runInfo.getBytesCopied());
            copyPartitionsCounter.incrementCompletionCount();
            return 0;
          case FAILED:
            return -1;
          default:
            throw new RuntimeException("State not handled: " + runInfo.getRunStatus());
        }
      } catch (HiveMetastoreException e) {
        LOG.error("Got an exception - will retry", e);
      } catch (DistCpException e) {
        LOG.error("Got an exception - will retry", e);
      } catch (IOException e) {
        LOG.error("Got an exception - will retry", e);
      } catch (ConfigurationException e) {
        LOG.error("Got unrecoverable exception", e);
        return -1;
      }
      LOG.error("Because " + copyPartitionTask.getSpec() + " was not successful, "
          + "it will be retried after sleeping.");
      try {
        ReplicationUtils.exponentialSleep(attempt);
      } catch (InterruptedException e) {
        LOG.warn("Got interrupted", e);
        return 0;
      }
      attempt++;
    }
  }

  @Override
  public LockSet getRequiredLocks() {
    return copyPartitionTask.getRequiredLocks();
  }

  @Override
  public String toString() {
    return "CopyPartitionJob{" + "spec=" + copyPartitionTask.getSpec() + '}';
  }
}
