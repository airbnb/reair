package com.airbnb.reair.multiprocessing;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Accepts a bunch of jobs, executes them in parallel while observing the locks that each jobs
 * needs.
 */
public class ParallelJobExecutor {
  private static final Log LOG = LogFactory.getLog(ParallelJobExecutor.class);

  private BlockingQueue<Job> jobsToRun;
  private JobDagManager dagManager;
  private int numWorkers = 0;
  private Set<Worker> workers = new HashSet<>();

  // Vars for counting the number of jobs
  // Lock to hold when incrementing either count
  private Lock countLock = new ReentrantLock();
  // Condition variable to signal when submitted and done counts are equal
  private Condition equalCountCv = countLock.newCondition();
  private int submittedJobCount = 0;
  private int doneJobCount = 0;

  private String workerName = "Worker";

  /**
   * Constructor for a job executor that run jobs in multiple threads.
   *
   * @param numWorkers the number of threads (i.e. workers) to create
   */
  public ParallelJobExecutor(int numWorkers) {
    dagManager = new JobDagManager();
    jobsToRun = new LinkedBlockingQueue<Job>();
    this.numWorkers = numWorkers;
  }

  /**
   * Constructor for a job executor that run jobs in multiple threads with the option to give a
   * prefix to the workers' thread names.
   *
   * @param workerName a prefix use for the worker thread name
   * @param numWorkers the number of threads (i.e. workers) to create
   */
  public ParallelJobExecutor(String workerName, int numWorkers) {
    this.workerName = workerName;
    dagManager = new JobDagManager();
    jobsToRun = new LinkedBlockingQueue<Job>();
    this.numWorkers = numWorkers;
  }

  /**
   * Add the given job to run. It will attempt to acquire the locks needed by the job, but if not
   * possible, it will wait until the jobs that hold the required locks give them up. With this
   * requirement in mind, jobs will be executed in the order that they are added.
   *
   * @param job the job that should be run
   */
  public synchronized void add(Job job) {
    boolean canRunImmediately = dagManager.addJob(job);
    if (canRunImmediately) {
      LOG.debug("Job " + job + " is ready to run.");
      jobsToRun.add(job);
    }
    incrementSubmittedJobCount();
  }


  /**
   * Should be called by the workers to indicate that a job has finished running. This removes the
   * job from the DAG so that other jobs that depended on the finished job can now be run.
   *
   * @param doneJob the job that is done running
   */
  public synchronized void notifyDone(Job doneJob) {
    LOG.debug("Done notification received for " + doneJob);
    Set<Job> newReadyJobs = dagManager.removeJob(doneJob);
    for (Job jobToRun : newReadyJobs) {
      LOG.debug("Job " + jobToRun + " is ready to run.");
      jobsToRun.add(jobToRun);
    }
    incrementDoneJobCount();

    countLock.lock();
    try {
      LOG.debug("Submitted jobs: " + submittedJobCount + " Pending jobs: "
          + (submittedJobCount - doneJobCount) + " Completed jobs: " + doneJobCount);
    } finally {
      countLock.unlock();
    }
  }

  /**
   * This is used with incrementJobDoneCount() to know when all the jobs submitted to the executor
   * has finished.
   */
  private void incrementSubmittedJobCount() {
    countLock.lock();
    try {
      submittedJobCount++;
    } finally {
      countLock.unlock();
    }
  }

  private void incrementDoneJobCount() {
    countLock.lock();
    try {
      doneJobCount++;
      if (doneJobCount == submittedJobCount) {
        equalCountCv.signal();
      }
    } finally {
      countLock.unlock();
    }
  }

  /**
   * Get the number of jobs that are not done.
   *
   * @return the number of jobs that are not done
   */
  public long getNotDoneJobCount() {
    countLock.lock();
    try {
      return submittedJobCount - doneJobCount;
    } finally {
      countLock.unlock();
    }
  }

  /**
   * Wait for the number of finished jobs to equal to the number of submitted jobs.
   */
  public void waitUntilDone() {
    countLock.lock();
    try {
      equalCountCv.await();
    } catch (InterruptedException e) {
      throw new RuntimeException("Shouldn't happen!");
    } finally {
      countLock.unlock();
    }
  }

  /**
   * Kick off the worker threads that run a job.
   */
  public synchronized void start() {
    if (workers.size() > 0) {
      throw new RuntimeException("Start called while there are workers" + " still running");
    }

    for (int i = 0; i < numWorkers; i++) {
      Worker worker = new Worker<Job>(workerName, jobsToRun, this);
      workers.add(worker);
    }

    for (Worker w : workers) {
      try {
        Thread.sleep(100);
      } catch (Exception e) {
        LOG.error(e);
      }
      w.start();
    }
  }

  /**
   * Interrupt the threads that are currently working on the jobs and wait for them to stop.
   *
   * @throws InterruptedException if interrupted while waiting for threads to finish
   */
  public synchronized void stop() throws InterruptedException {
    for (Worker w : workers) {
      w.interrupt();
    }

    for (Worker w : workers) {
      w.join();
    }

    // Do this after interrupting? Think about case when a worker takes an
    // item from the queue and is then interrupted.
    for (Worker w : workers) {
      if (w.getJob() != null) {
        jobsToRun.add(w.getJob());
      }
    }
    workers.clear();
  }
}
