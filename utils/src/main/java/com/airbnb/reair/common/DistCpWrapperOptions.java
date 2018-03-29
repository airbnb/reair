package com.airbnb.reair.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;

import java.util.Collections;
import java.util.List;
import java.util.PriorityQueue;

/**
 * A class to encapsulate various options required for running DistCp.
 */
public class DistCpWrapperOptions {
  private static final Log LOG = LogFactory.getLog(DistCpWrapperOptions.class);

  // The source directory to copy
  private Path srcDir;
  // The destination directory for the copy
  private Path destDir;
  // Where distcp should temporarily copy files to
  private Path distCpTmpDir;
  // The log directory for the distcp job
  private Path distCpLogDir;
  // If atomic, distCp will copy to a temporary directory first and then
  // do a directory move to the final location
  private boolean atomic = true;
  // If the destination directory exists with different data, can it be
  // deleted?
  private boolean canDeleteDest = true;
  // Whether to set the modification times to be the same for the copied files
  private boolean syncModificationTimes = true;
  // Size number of mappers for the distcp job based on the source directory
  // size and the number of files.
  private long bytesPerMapper = (long) 256e6;
  private int filesPerMapper = 100;
  // If the distCp job runs longer than this many ms, fail the job
  private long distcpJobTimeout = 1800 * 1000;
  // If the input data size is smaller than this many MB, and fewer than
  // this many files, use a local -cp command to copy the files.
  private long localCopyCountThreshold = (long) 100;
  private long localCopySizeThreshold = (long) 256e6;
  // Poll for the progress of DistCp every N ms
  private long distCpPollInterval = 2500;
  // Use a variable amount of time for distcp job timeout, depending on filesize
  // subject to a minimum and maximum
  // ceil(filesize_gb) * timeoutMsPerGb contrained to range (min, max)
  private boolean distcpDynamicJobTimeoutEnabled = false;
  // timeout in millis per GB per mapper, size will get rounded up
  private long distcpDynamicJobTimeoutMsPerGbPerMapper = 0;
  // minimum job timeout for variable timeout (ms) which accounts for overhead
  private long distcpDynamicJobTimeoutBase = distcpJobTimeout;
  // maximum job timeout for variable timeout (ms)
  private long distcpDynamicJobTimeoutMax = Long.MAX_VALUE;

  /**
   * Constructor for DistCp options.
   *
   * @param srcDir the source directory to copy from
   * @param destDir the destination directory to copy to
   * @param distCpTmpDir the temporary directory to use when copying
   * @param distCpLogDir the log directory to use when copying
   */
  public DistCpWrapperOptions(Path srcDir, Path destDir, Path distCpTmpDir, Path distCpLogDir) {
    this.srcDir = srcDir;
    this.destDir = destDir;
    this.distCpTmpDir = distCpTmpDir;
    this.distCpLogDir = distCpLogDir;
  }

  public DistCpWrapperOptions setAtomic(boolean atomic) {
    this.atomic = atomic;
    return this;
  }

  public DistCpWrapperOptions setCanDeleteDest(boolean canDeleteDest) {
    this.canDeleteDest = canDeleteDest;
    return this;
  }

  public DistCpWrapperOptions setSyncModificationTimes(boolean syncModificationTimes) {
    this.syncModificationTimes = syncModificationTimes;
    return this;
  }

  public DistCpWrapperOptions setBytesPerMapper(long bytesPerMapper) {
    this.bytesPerMapper = bytesPerMapper;
    return this;
  }

  public DistCpWrapperOptions setDistCpJobTimeout(long distCpJobTimeout) {
    this.distcpJobTimeout = distCpJobTimeout;
    return this;
  }

  public DistCpWrapperOptions setLocalCopySizeThreshold(long localCopySizeThreshold) {
    this.localCopySizeThreshold = localCopySizeThreshold;
    return this;
  }

  public DistCpWrapperOptions setDistcpDynamicJobTimeoutEnabled(
      boolean distcpDynamicJobTimeoutEnabled) {
    this.distcpDynamicJobTimeoutEnabled = distcpDynamicJobTimeoutEnabled;
    return this;
  }

  public DistCpWrapperOptions setDistcpDynamicJobTimeoutMsPerGbPerMapper(
      long distcpDynamicJobTimeoutMsPerGbPerMapper) {
    this.distcpDynamicJobTimeoutMsPerGbPerMapper = distcpDynamicJobTimeoutMsPerGbPerMapper;
    return this;
  }

  public DistCpWrapperOptions setDistcpDynamicJobTimeoutBase(
      long distcpDynamicJobTimeoutBase) {
    this.distcpDynamicJobTimeoutBase = distcpDynamicJobTimeoutBase;
    return this;
  }

  public DistCpWrapperOptions setDistcpDynamicJobTimeoutMax(
      long distcpDynamicJobTimeoutMax) {
    this.distcpDynamicJobTimeoutMax = distcpDynamicJobTimeoutMax;
    return this;
  }

  public Path getSrcDir() {
    return srcDir;
  }

  public Path getDestDir() {
    return destDir;
  }

  public Path getDistCpTmpDir() {
    return distCpTmpDir;
  }

  public Path getDistCpLogDir() {
    return distCpLogDir;
  }

  public boolean getAtomic() {
    return atomic;
  }

  public boolean getCanDeleteDest() {
    return canDeleteDest;
  }

  public boolean getSyncModificationTimes() {
    return syncModificationTimes;
  }

  public long getBytesPerMapper() {
    return bytesPerMapper;
  }

  public int getFilesPerMapper() {
    return filesPerMapper;
  }

  public long getLocalCopySizeThreshold() {
    return localCopySizeThreshold;
  }

  public long getLocalCopyCountThreshold() {
    return localCopyCountThreshold;
  }

  public long getDistCpPollInterval() {
    return distCpPollInterval;
  }

  /**
   * Returns the distcp timeout in milliseconds according to options set.
   * @param fileSizes File sizes of the files to copy.
   * @param maxConcurrency The number of mappers in distcp.
   * @return The timeout in milliseconds for distcp.
   */
  public long getDistcpTimeout(List<Long> fileSizes, long maxConcurrency) {
    if (distcpDynamicJobTimeoutEnabled) {
      long bytesPerLongestMapper = computeLongestMapper(fileSizes, maxConcurrency);
      long baseTimeout = distcpDynamicJobTimeoutBase;
      long maxTimeout = distcpDynamicJobTimeoutMax;
      long msPerGb = distcpDynamicJobTimeoutMsPerGbPerMapper;
      long adjustment = ((long) Math.ceil(bytesPerLongestMapper / 1e9) * msPerGb);
      long timeout = Math.min(maxTimeout, baseTimeout + adjustment);
      LOG.debug(String.format("Setting dynamic timeout of %d milliseconds for max mapper size %d",
          timeout, bytesPerLongestMapper));
      return timeout;
    } else {
      return distcpJobTimeout;
    }
  }

  /**
   * Computes an estimate for how many bytes the mapper that copies the most will copy.
   * This is within 4/3 of the optimal scheduling using a heuristic for the multiprocessor
   * scheduling problem.
   * @param fileSizes A list of filesizes to copy.
   * @param maxConcurrency How many parallel processes will copy the files.
   * @return An estimate of how many bytes the busiest mapper will copy.
   */
  public long computeLongestMapper(List<Long> fileSizes, long maxConcurrency) {
    Collections.sort(fileSizes);
    PriorityQueue<Long> processors = new PriorityQueue<>();
    for (int i = 0; i < maxConcurrency; i++) {
      processors.add(0L);
    }
    Long maxValue = 0L;
    for (int i = fileSizes.size() - 1; i >= 0; i--) {
      Long popped = processors.poll();
      Long newValue = popped + fileSizes.get(i);
      processors.add(newValue);
      maxValue = Math.max(maxValue, newValue);
    }
    return maxValue;
  }
}
