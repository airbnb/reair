package com.airbnb.reair.incremental;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.TreeMap;

import com.timgroup.statsd.StatsDClient;

/**
 * Keeps track of a set of jobs.
 */
public class ReplicationJobRegistry {
  private static final long[] thresholds = {1800, 3600, 7200, 10800, 21600};

  private static long MAX_RETIRED_JOBS = 200;
  private StatsDClient statsDClient;

  TreeMap<Long, ReplicationJob> idToReplicationJob = new TreeMap<>();

  LinkedList<ReplicationJob> retiredJobs = new LinkedList<>();

  public ReplicationJobRegistry(StatsDClient statsDClient) {
    this.statsDClient = statsDClient;
  }

  public synchronized void registerJob(ReplicationJob job) {
    idToReplicationJob.put(job.getId(), job);
  }

  public synchronized ReplicationJob getJob(long id) {
    return idToReplicationJob.get(id);
  }

  /**
   * Get the job with the smallest ID value in the registry. The job with the smallest ID is
   * generally the oldest job.
   *
   * @return the job with the smallest ID in the registry
   */
  public synchronized ReplicationJob getJobWithSmallestId() {
    if (idToReplicationJob.size() == 0) {
      return null;
    } else {
      return idToReplicationJob.firstEntry().getValue();
    }

  }

  /**
   *
   * @return a collection containing all the active replication jobs. The jobs are returned ordered
   *         by id ascending.
   */
  public synchronized Collection<ReplicationJob> getActiveJobs() {
    return new ArrayList<>(idToReplicationJob.values());
  }

  /**
   * Remove this job from the main internal data structures to another retired job datastructure.
   *
   * @param job the job to remove
   */
  public synchronized void retireJob(ReplicationJob job) {
    ReplicationJob removedJob = idToReplicationJob.remove(job.getId());

    if (removedJob == null) {
      throw new RuntimeException("Couldn't find id: " + job.getId() + " in the registry!");
    }

    if (removedJob != job) {
      throw new RuntimeException("Replication jobs with the same ID " + "are not equal: %s and %s");
    }
    // Trim the size of the list so that we exceed the limit.
    if (retiredJobs.size() + 1 > MAX_RETIRED_JOBS) {
      retiredJobs.remove(0);
    }
    retiredJobs.add(removedJob);
  }

  public synchronized Collection<ReplicationJob> getRetiredJobs() {
    return new ArrayList<>(retiredJobs);
  }

  public synchronized void reportStats() {
    long now = System.currentTimeMillis() / 1000;
    Map<Long, Integer> mapCount = new HashMap<>();
    for (Long value: thresholds) {
      mapCount.put(value, 0);
    }
    for (ReplicationJob job : idToReplicationJob.values()) {
      if (job.getPersistedJobInfo().getSrcObjectTldt().isPresent()) {
        long time = Long.parseLong(job.getPersistedJobInfo().getSrcObjectTldt().get());
        for (Long value: thresholds) {
          if (time - now > value) {
            mapCount.put(value, mapCount.get(value) + 1);
          }
        }
      }
    }
    for (Map.Entry<Long, Integer> val: mapCount.entrySet()) {
      statsDClient.gauge("replication_jobs.age." + val.getKey() + "s", val.getValue());
    }
  }

}
