package com.airbnb.reair.incremental;

import com.airbnb.reair.common.DistCpException;
import com.airbnb.reair.common.DistCpWrapper;
import com.airbnb.reair.common.DistCpWrapperOptions;
import com.airbnb.reair.common.FsUtils;
import com.airbnb.reair.common.PathBuilder;
import com.airbnb.reair.incremental.configuration.ConfigurationException;
import com.airbnb.reair.incremental.deploy.ConfigurationKeys;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.Random;

/**
 * Copies directories on Hadoop filesystems.
 */
public class DirectoryCopier {
  private static final Log LOG = LogFactory.getLog(DirectoryCopier.class);

  private Configuration conf;
  private Path tmpDir;
  private boolean checkFileModificationTimes;

  /**
   * Constructor for the directory copier.
   *
   * @param conf configuration object
   * @param tmpDir the temporary directory to copy data to before moving to the final destination
   * @param checkFileModificationTimes Whether to check that the modified times of the files match
   *                                   after the copy. Some filesystems do not support preservation
   *                                   of modified file time after a copy, so this check may need to
   *                                   be disabled.
   */
  public DirectoryCopier(Configuration conf, Path tmpDir, boolean checkFileModificationTimes) {
    this.conf = conf;
    this.tmpDir = tmpDir;
    this.checkFileModificationTimes = checkFileModificationTimes;
  }

  /**
   * Copy the source directory to the destination directory.
   *
   * @param srcDir source directory
   * @param destDir destination directory
   * @param copyAttributes a list of attributes to use when creating the tmp directory. Doesn't
   *        really matter, but it can make it easier to manually inspect the tmp directory.
   * @return the number of bytes copied
   * @throws IOException if there was an error copying the directory
   * @throws ConfigurationException if configuration options are improper
   */
  public long copy(Path srcDir, Path destDir, List<String> copyAttributes)
      throws ConfigurationException, IOException {
    Random random = new Random();
    long randomLong = Math.abs(random.nextLong());

    PathBuilder tmpDirPathBuilder = new PathBuilder(tmpDir).add("distcp_tmp");
    for (String attribute : copyAttributes) {
      tmpDirPathBuilder.add(attribute);
    }
    Path distCpTmpDir = tmpDirPathBuilder.add(Long.toHexString(randomLong)).toPath();

    PathBuilder logDirPathBuilder = new PathBuilder(tmpDir).add("distcp_logs");

    for (String attribute : copyAttributes) {
      logDirPathBuilder.add(attribute);
    }

    Path distCpLogDir = logDirPathBuilder.add(Long.toHexString(randomLong)).toPath();

    try {
      // Copy directory
      DistCpWrapperOptions options =
          new DistCpWrapperOptions(srcDir, destDir, distCpTmpDir, distCpLogDir)
              .setAtomic(true)
              .setSyncModificationTimes(checkFileModificationTimes);

      long copyJobTimeoutSeconds = conf.getLong(
          ConfigurationKeys.COPY_JOB_TIMEOUT_SECONDS,
          -1);
      if (copyJobTimeoutSeconds > 0) {
        options.setDistCpJobTimeout(copyJobTimeoutSeconds * 1_000L);
      }
      boolean dynamicTimeoutEnabled = conf.getBoolean(
          ConfigurationKeys.COPY_JOB_DYNAMIC_TIMEOUT_ENABLED,
          false);
      options.setDistcpDynamicJobTimeoutEnabled(dynamicTimeoutEnabled);
      if (dynamicTimeoutEnabled && copyJobTimeoutSeconds > 0) {
        throw new ConfigurationException(String.format(
            "The config options {} and {} are both set, but only one can be used",
            ConfigurationKeys.COPY_JOB_DYNAMIC_TIMEOUT_ENABLED,
            ConfigurationKeys.COPY_JOB_TIMEOUT_SECONDS));
      }
      long dynamicTimeoutMsPerGbPerMapper = conf.getLong(
          ConfigurationKeys.COPY_JOB_DYNAMIC_TIMEOUT_MS_PER_GB_PER_MAPPER,
          -1);
      if (dynamicTimeoutMsPerGbPerMapper > 0) {
        options.setDistcpDynamicJobTimeoutMsPerGbPerMapper(dynamicTimeoutMsPerGbPerMapper);
      }
      long dynamicTimeoutMin = conf.getLong(
          ConfigurationKeys.COPY_JOB_DYNAMIC_TIMEOUT_BASE,
          -1);
      if (dynamicTimeoutMin > 0) {
        options.setDistcpDynamicJobTimeoutBase(dynamicTimeoutMin);
      }
      long dynamicTimeoutMax = conf.getLong(
          ConfigurationKeys.COPY_JOB_DYNAMIC_TIMEOUT_MAX,
          -1);
      if (dynamicTimeoutMax > 0) {
        options.setDistcpDynamicJobTimeoutMax(dynamicTimeoutMax);
      }

      DistCpWrapper distCpWrapper = new DistCpWrapper(conf);
      long bytesCopied = distCpWrapper.copy(options);
      return bytesCopied;
    } catch (DistCpException e) {
      throw new IOException(e);
    }
  }

  /**
   * Checks to see if two directories contain the same files. Same is defined as having the same set
   * of non-empty files with matching file sizes (and matching modified times if set in the
   * constructor)
   *
   * @param srcDir source directory
   * @param destDir destination directory
   * @return whether directories are equal
   *
   * @throws IOException if there's an error reading the filesystem
   */
  public boolean equalDirs(Path srcDir, Path destDir) throws IOException {
    return FsUtils.equalDirs(conf, srcDir, destDir, Optional.empty(), checkFileModificationTimes);
  }
}
