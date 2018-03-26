package com.airbnb.reair.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.tools.DistCp;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * This is a wrapper around DistCp that adds a few options and makes it easier to use.
 */
public class DistCpWrapper {

  private static final Log LOG = LogFactory.getLog(DistCpWrapper.class);

  private Configuration conf;

  public DistCpWrapper(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Constructor using the specified options.
   *
   * @param options the options to use when copying
   * @return the number of bytes copied
   *
   * @throws IOException if there's an error accessing the filesystem
   * @throws DistCpException if there is an error running DistCp
   */
  public long copy(DistCpWrapperOptions options) throws IOException, DistCpException {

    if (Thread.currentThread().isInterrupted()) {
      throw new DistCpException("Current thread has been interrupted");
    }

    Path srcDir = options.getSrcDir();
    Path destDir = options.getDestDir();
    Path distCpTmpDir = options.getDistCpTmpDir();
    Path distCpLogDir = options.getDistCpLogDir();

    boolean destDirExists = FsUtils.dirExists(conf, destDir);
    LOG.debug("Dest dir " + destDir + " exists is " + destDirExists);

    boolean syncModificationTimes = options.getSyncModificationTimes();
    boolean atomic = options.getAtomic();
    boolean canDeleteDest = options.getCanDeleteDest();

    if (destDirExists
        && FsUtils.equalDirs(conf, srcDir, destDir, Optional.empty(), syncModificationTimes)) {
      LOG.debug("Source and destination paths are already equal!");
      return 0;
    }

    boolean useDistcpUpdate = false;
    // Distcp -update can be used for cases where we're not doing an atomic
    // copy and there aren't any files in the destination that are not in
    // the source. If you delete specific files on the destination, it's
    // possible to do distcp update with unique files in the dest. However,
    // that functionality is not yet built out. Instead, this deletes the
    // destination directory and does a fresh copy.
    if (!atomic) {
      useDistcpUpdate = destDirExists
          && !FsUtils.filesExistOnDestButNotSrc(conf, srcDir, destDir, Optional.empty());
      if (useDistcpUpdate) {
        LOG.debug("Doing a distcp update from " + srcDir + " to " + destDir);
      }
    }

    if (destDirExists && !canDeleteDest && !useDistcpUpdate) {
      throw new IOException("Destination directory (" + destDir
          + ") exists, can't use update, and can't " + "overwrite!");
    }

    if (destDirExists && canDeleteDest && !useDistcpUpdate && !atomic) {
      LOG.debug("Unable to use distcp update, so deleting " + destDir + " since it already exists");
      FsUtils.deleteDirectory(conf, destDir);
    }

    Path distcpDestDir;
    // For atomic moves, copy to a temporary location and then move the
    // directory to the final destination. Note: S3 doesn't support atomic
    // directory moves so don't use this option for S3 destinations.
    if (atomic) {
      distcpDestDir = distCpTmpDir;
    } else {
      distcpDestDir = destDir;
    }

    LOG.debug(String.format("Copying %s to %s", srcDir, distcpDestDir));


    Set<FileStatus> fileStatuses =
        FsUtils.getFileStatusesRecursive(conf, srcDir, Optional.empty());
    List<Long> fileSizes = new ArrayList<>();

    long srcSize = 0;
    for (FileStatus status : fileStatuses) {
      srcSize += status.getLen();
      fileSizes.add(status.getLen());
    }
    LOG.debug(String.format(
        "%s has %s files with a total size of %s bytes",
        srcDir, fileStatuses.size(), srcSize));

    // Use shell to copy for small files
    if (srcSize < options.getLocalCopySizeThreshold()
        && fileStatuses.size() < options.getLocalCopyCountThreshold()) {
      String[] mkdirArgs = {"-mkdir", "-p", distcpDestDir.getParent().toString()};
      String[] copyArgs = {"-cp", srcDir.toString(), distcpDestDir.toString()};

      FsShell shell = new FsShell();
      try {
        LOG.debug("Using shell to mkdir with args " + Arrays.asList(mkdirArgs));
        ToolRunner.run(shell, mkdirArgs);
        LOG.debug("Using shell to copy with args " + Arrays.asList(copyArgs));
        ToolRunner.run(shell, copyArgs);
      } catch (Exception e) {
        throw new DistCpException(e);
      } finally {
        shell.close();
      }

      if (syncModificationTimes) {
        FsUtils.syncModificationTimes(conf, srcDir, distcpDestDir, Optional.empty());
      }
    } else {

      LOG.debug("DistCp log dir: " + distCpLogDir);
      LOG.debug("DistCp dest dir: " + distcpDestDir);
      LOG.debug("DistCp tmp dir: " + distCpTmpDir);
      // Make sure that the tmp dir and the destination directory are on
      // the same schema
      if (!FsUtils.sameFs(distCpTmpDir, distcpDestDir)) {
        throw new DistCpException(
            String.format("Filesystems do not match for tmp (%s) " + "and destination (%s)",
                distCpTmpDir, distcpDestDir));
      }

      List<String> distcpArgs = new ArrayList<>();
      distcpArgs.add("-m");
      long mappers = Math.max(1, srcSize / options.getBytesPerMapper());
      mappers = Math.max(mappers, fileStatuses.size() / options.getFilesPerMapper());
      distcpArgs.add(Long.toString(mappers));
      distcpArgs.add("-log");
      distcpArgs.add(distCpLogDir.toString());
      if (useDistcpUpdate) {
        distcpArgs.add("-update");
      }
      // Preserve replication number, user, group, permissions, and block size. 
      // Preserving block size is needed for DistCp to use built in checksums for verification.
      distcpArgs.add("-prugpb");
      distcpArgs.add(srcDir.toString());
      distcpArgs.add(distcpDestDir.toString());
      LOG.debug("Running DistCp with args: " + distcpArgs);

      // For distcp v1, do something like
      // DistCp distCp = new DistCp(conf);

      // For distcp v2
      DistCp distCp = new DistCp();
      distCp.setConf(conf);

      long distCpTimeout = options.getDistcpTimeout(fileSizes, mappers);

      int ret = runDistCp(distCp, distcpArgs, distCpTimeout,
          options.getDistCpPollInterval());

      if (Thread.currentThread().isInterrupted()) {
        throw new DistCpException("Thread interrupted");
      }

      if (ret != 0) {
        throw new DistCpException("Distcp failed");
      }
    }

    if (syncModificationTimes) {
      FsUtils.syncModificationTimes(conf, srcDir, distcpDestDir, Optional.empty());
    }

    if (!FsUtils.equalDirs(conf, srcDir, distcpDestDir, Optional.empty(), syncModificationTimes)) {
      LOG.error("Source and destination sizes don't match!");
      if (atomic) {
        LOG.debug("Since it's an atomic copy, deleting " + distcpDestDir);
        FsUtils.deleteDirectory(conf, distcpDestDir);
        throw new DistCpException("distcp result mismatch");
      }
    } else {
      LOG.debug("Size of source and destinations match");
    }

    if (atomic) {
      // Size is good, clear out the final destination directory and
      // replace with the copied version.
      destDirExists = FsUtils.dirExists(conf, destDir);
      if (destDirExists) {
        LOG.debug("Deleting existing directory " + destDir);
        FsUtils.deleteDirectory(conf, destDir);
      }
      LOG.debug("Moving from " + distCpTmpDir + " to " + destDir);
      FsUtils.moveDir(conf, distcpDestDir, destDir);
    }

    LOG.debug("Deleting log directory " + distCpLogDir);
    FsUtils.deleteDirectory(conf, distCpLogDir);

    // Not necessarily the bytes copied if using -update
    return srcSize;
  }

  /**
   * Run distcp in a separate thread, but kill the thread if runtime exceeds timeout.
   *
   * @param distCp directory copier object
   * @param options the command line arguments to pass to DistCp
   * @param timeout the maximum number of miliseconds that DistCp should run
   * @param pollInterval how frequently to check if DistCp is done
   * @return the value returned by DistCp
   *
   * @throws InterruptedException if this thread is interrupted while waiting for DistCp
   */
  private int runDistCp(final DistCp distCp, final List<String> options, long timeout,
      long pollInterval) throws DistCpException {

    // Kick off distcp in a separate thread so we can implement a timeout
    final Container<Integer> retVal = new Container<Integer>();
    Thread distCpRunner = new Thread() {
      @Override
      public void run() {
        int ret = distCp.run(options.toArray(new String[] {}));
        retVal.set(Integer.valueOf(ret));
      }
    };
    distCpRunner.setDaemon(true);
    distCpRunner.setName(Thread.currentThread().getName() + "-distcp-" + distCpRunner.getId());
    distCpRunner.start();

    long startTime = System.currentTimeMillis();
    while (true) {
      if (System.currentTimeMillis() - startTime > timeout) {
        LOG.debug(String.format("DistCp exceeded timeout of %sms", timeout));
        distCpRunner.interrupt();
        break;
      }

      if (retVal.get() != null) {
        break;
      }

      try {
        Thread.sleep(pollInterval);
      } catch (InterruptedException e) {
        throw new DistCpException(e);
      }
    }

    return retVal.get() == null ? -1 : retVal.get();
  }
}
