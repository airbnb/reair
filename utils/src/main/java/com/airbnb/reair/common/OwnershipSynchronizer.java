package com.airbnb.reair.common;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

public class OwnershipSynchronizer {

  private static final Log LOG = LogFactory.getLog(OwnershipSynchronizer.class);

  private final Configuration conf;

  public OwnershipSynchronizer(Configuration conf) {
    this.conf = conf;
  }

  /**
   * Synchronize owner and group of destination files.
   *
   * @param src Source file
   * @param dest Destination file
   * @param filter Filter
   * @throws IOException If setting owner fails
     */
  public void syncOwnership(Path src, Path dest,
                Optional<PathFilter> filter) throws IOException {

    Map<String, FileOwnership> srcFileOwners = null;
    Set<String> syncedParents = new HashSet<>();
    Set<FileStatus> srcFileStatuses = FsUtils.getFileStatusesRecursive(conf, src, filter);

    try {
      srcFileOwners = getRelativePathToOwner(conf, src, srcFileStatuses);
    } catch (ArgumentException e) {
      throw new IOException("Invalid file statuses!", e);
    }

    FileSystem destFs = dest.getFileSystem(conf);
    for (String file : srcFileOwners.keySet()) {
      FileOwnership srcFile = srcFileOwners.get(file);

      Path destFile = new Path(dest, file);
      setOwnership(destFs, destFile, srcFile.getOwner(), srcFile.getGroup());

      Path destParentFile = destFile.getParent();
      String destParentFilePath = destParentFile.toString();
      if (!syncedParents.contains(destParentFilePath)) {
        setOwnership(destFs, destParentFile, srcFile.getParentOwner(), srcFile.getParentGroup());
        syncedParents.add(destParentFilePath);
      }
    }
  }

  private void setOwnership(FileSystem destFs, Path destFile, String owner, String group)
          throws IOException {
    LOG.debug("Setting ownership for: " + destFile.toString() + " to: " + owner + ":" + group);
    destFs.setOwner(destFile, owner, group);
  }

  private Map<String, FileOwnership> getRelativePathToOwner(
      Configuration conf,
      Path root,
      Set<FileStatus> statuses) throws ArgumentException, IOException {
    Map<String, FileOwnership> pathToStatus = new HashMap<>();

    FileSystem srcFs = FileSystem.get(root.toUri(), conf);

    for (FileStatus status : statuses) {
      Path path = status.getPath();
      Path parentPath = path.getParent();
      FileStatus parentStatus = srcFs.getFileStatus(parentPath);
      pathToStatus.put(
              FsUtils.getRelativePath(root, path),
              new FileOwnership(status, parentStatus)
      );
    }
    return pathToStatus;
  }

  static class FileOwnership {
    private final String owner;
    private final String group;
    private final String parentOwner;
    private final String parentGroup;

    public FileOwnership(FileStatus status, FileStatus parentStatus) {
      this.owner = status.getOwner();
      this.group = status.getGroup();
      this.parentOwner = parentStatus.getOwner();
      this.parentGroup = parentStatus.getGroup();
    }

    public String getOwner() {
      return owner;
    }

    public String getGroup() {
      return group;
    }

    public String getParentOwner() {
      return parentOwner;
    }

    public String getParentGroup() {
      return parentGroup;
    }

    @Override
    public boolean equals(Object other) {
      if (this == other) {
        return true;
      }
      if (other == null || getClass() != other.getClass()) {
        return false;
      }
      FileOwnership that = (FileOwnership) other;
      return Objects.equals(owner, that.owner)
              && Objects.equals(group, that.group)
              && Objects.equals(parentOwner, that.parentOwner)
              && Objects.equals(parentGroup, that.parentGroup);
    }

    @Override
    public int hashCode() {
      return Objects.hash(owner, group, parentOwner, parentGroup);
    }
  }
}
