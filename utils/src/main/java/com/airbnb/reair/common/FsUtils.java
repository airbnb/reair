package com.airbnb.reair.common;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileChecksum;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.Trash;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;

/**
 * Utility methods related to the file system.
 */
public class FsUtils {

  private static final Log LOG = LogFactory.getLog(FsUtils.class);

  public static boolean sameFs(Path p1, Path p2) {
    return StringUtils.equals(p1.toUri().getScheme(), p2.toUri().getScheme())
        && StringUtils.equals(p1.toUri().getAuthority(), p2.toUri().getAuthority());
  }

  /**
   * Get the total size of the files under the specified path.
   *
   * @param conf the configuration object
   * @param path the path to get the size of
   * @param filter use this to filter out files and directories
   * @return the size of the given location in bytes, including the size of any subdirectories
   *
   * @throws IOException if there's an error accessing the filesystem
   */
  public static long getSize(Configuration conf, Path path, Optional<PathFilter> filter)
      throws IOException {
    long totalSize = 0;

    FileSystem fs = FileSystem.get(path.toUri(), conf);

    Queue<Path> pathsToCheck = new LinkedList<>();
    pathsToCheck.add(path);

    // Traverse the directory tree and find all the paths
    // Use this instead of listFiles() as there seems to be more errors
    // related to block locations when using with s3n
    while (pathsToCheck.size() > 0) {
      Path pathToCheck = pathsToCheck.remove();
      if (filter.isPresent() && !filter.get().accept(pathToCheck)) {
        LOG.warn("Skipping check of directory: " + pathToCheck);
        continue;
      }
      FileStatus[] statuses = fs.listStatus(pathToCheck);
      for (FileStatus status : statuses) {
        if (status.isDirectory()) {
          pathsToCheck.add(status.getPath());
        } else {
          totalSize += status.getLen();
        }
      }
    }
    return totalSize;
  }

  /**
   * Check if a directory exceeds the specified size.
   *
   * @param conf configuration object
   * @param path the path to check the size of
   * @param maxSize max size for comparison
   * @return whether the specified size exceeds the max size
   *
   * @throws IOException if there's an error accessing the filesystem
   */
  public static boolean exceedsSize(Configuration conf, Path path, long maxSize)
      throws IOException {
    long totalSize = 0;
    FileSystem fs = FileSystem.get(path.toUri(), conf);

    Queue<Path> pathsToCheck = new LinkedList<>();
    pathsToCheck.add(path);

    // Traverse the directory tree and find all the paths
    // Use this instead of listFiles() as there seems to be more errors
    // related to block locations when using with s3n
    while (pathsToCheck.size() > 0) {
      Path pathToCheck = pathsToCheck.remove();
      FileStatus[] statuses = fs.listStatus(pathToCheck);
      for (FileStatus status : statuses) {
        if (status.isDirectory()) {
          pathsToCheck.add(status.getPath());
        } else {
          totalSize += status.getLen();
          if (totalSize > maxSize) {
            return true;
          }
        }
      }
    }
    return false;
  }

  /**
   * Get the file statuses of all the files in the path, including subdirectories.
   *
   * @param conf configuration object
   * @param path the path to examine
   *
   * @return the FileStatus of all the files in the supplied path, including subdirectories
   * @throws IOException if there's an error accessing the filesystem
   */
  public static Set<FileStatus> getFileStatusesRecursive(
      Configuration conf,
      Path path,
      Optional<PathFilter> filter) throws IOException {
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    Set<FileStatus> fileStatuses = new HashSet<>();

    Queue<Path> pathsToCheck = new LinkedList<>();
    pathsToCheck.add(path);

    // Traverse the directory tree and find all the paths
    // Use this instead of listFiles() as there seems to be more errors
    // related to block locations when using with s3n
    while (pathsToCheck.size() > 0) {
      Path pathToCheck = pathsToCheck.remove();
      if (filter.isPresent() && !filter.get().accept(pathToCheck)) {
        LOG.warn("Skipping check of directory: " + pathToCheck);
        continue;
      }
      FileStatus[] statuses = fs.listStatus(pathToCheck);
      for (FileStatus status : statuses) {
        if (status.isDirectory()) {
          pathsToCheck.add(status.getPath());
        } else {
          fileStatuses.add(status);
        }
      }
    }
    return fileStatuses;
  }

  /**
   * Get the sizes of the files under the specified path.
   *
   * @param root the path to check
   * @param statuses a set of statuses for all the files in the root directory
   * @return a map from the path to a file relative to the root (e.g. a/b.txt) to the associated
   *         file size
   */
  private static Map<String, Long> getRelPathToSizes(Path root, Set<FileStatus> statuses)
      throws ArgumentException {
    Map<String, Long> pathToStatus = new HashMap<>();
    for (FileStatus status : statuses) {
      pathToStatus.put(getRelativePath(root, status.getPath()), status.getLen());
    }
    return pathToStatus;
  }

  /**
   * Get the modified times of the files under the specified path.
   *
   * @param root the path to check
   * @param statuses a set of statuses for all the files in the root directory
   * @return a map from the path to a file relative to the root (e.g. a/b.txt) to the associated
   *         modification time
   */
  private static Map<String, Long> getRelativePathToModificationTime(Path root,
                                                                     Set<FileStatus> statuses)
      throws ArgumentException {
    Map<String, Long> pathToStatus = new HashMap<>();
    for (FileStatus status : statuses) {
      pathToStatus.put(getRelativePath(root, status.getPath()), status.getModificationTime());
    }
    return pathToStatus;
  }

  /**
   * Add "/" to path if path doesn't end with "/".
   */
  public static String getPathWithSlash(String path) {
    if (path == null) {
      return null;
    }
    if (!path.endsWith("/")) {
      path = path + "/";
    }
    return path;
  }

  /**
   * Get the path relative to another path.
   *
   * @param root the reference path
   * @param child a path under root
   * @return The relative path of the child given the root. For example, if the root was '/a' and
   *         the file was '/a/b/c.txt', the relative path would be 'b/c.txt'
   */
  public static String getRelativePath(Path root, Path child) {
    // TODO: Use URI.relativize()
    String prefix = getPathWithSlash(root.toString());
    if (!child.toString().startsWith(prefix)) {
      throw new RuntimeException("Invalid root: " + root + " and child " + child);
    }
    return child.toString().substring(prefix.length());
  }

  /**
   * Get the total size of the paths.
   *
   * @param relPathToSize a map from the relative path to a file to the file size
   * @return the total size of the files described in the map
   */
  private static long totalSize(Map<String, Long> relPathToSize) {
    long total = 0;
    for (Long l : relPathToSize.values()) {
      total += l;
    }
    return total;
  }

  /**
   * Checks to see if filenames exist on a destination directory that don't exist in the source
   * directory. Mainly used for checking if a distcp -update can work.
   *
   * @param conf configuration object
   * @param src source path
   * @param dest destination path
   * @param filter filter to use when traversing through the directories
   * @return true if there are any file names on the destination directory that are not in the
   *         source directory
   * @throws IOException if there's an error accesing the filesystem
   */
  public static boolean filesExistOnDestButNotSrc(Configuration conf, Path src, Path dest,
      Optional<PathFilter> filter) throws IOException {
    Set<FileStatus> srcFileStatuses = getFileStatusesRecursive(conf, src, filter);
    Set<FileStatus> destFileStatuses = getFileStatusesRecursive(conf, dest, filter);

    Map<String, Long> srcFileSizes = null;
    Map<String, Long> destFileSizes = null;

    try {
      srcFileSizes = getRelPathToSizes(src, srcFileStatuses);
      destFileSizes = getRelPathToSizes(dest, destFileStatuses);
    } catch (ArgumentException e) {
      throw new IOException("Invalid file statuses!", e);
    }

    for (String file : destFileSizes.keySet()) {
      if (!srcFileSizes.containsKey(file)) {
        LOG.warn(String.format("%s exists on %s but not in %s", file, dest, src));
        return true;
      }
    }
    return false;
  }

  public static boolean equalDirs(Configuration conf, Path src, Path dest) throws IOException {
    return equalDirs(conf, src, dest, Optional.empty());
  }

  /**
   * Checks to see if two directories are equal. The directories are considered equal if they have
   * the same non-zero files with the same sizes in the same paths.
   *
   * @param conf configuration object
   * @param src source directory
   * @param dest destination directory
   * @param filter files or directories rejected by this fileter are not checked
   * @return true if the files in the source and the destination are the 'same'. 'same' is defined
   *         as having the same set of files with matching sizes.
   * @throws IOException if there's an error accessing the filesystem
   */
  public static boolean equalDirs(Configuration conf, Path src, Path dest,
      Optional<PathFilter> filter) throws IOException {
    return equalDirs(conf, src, dest, filter, false);
  }

  /**
   * Checks to see if two directories are equal. The directories are considered equal if they have
   * the same non-zero files with the same sizes in the same paths (with the same modification times
   * if applicable)
   *
   * @param conf configuration object
   * @param src source directory
   * @param dest destination directory
   * @param filter filter for excluding some files from comparison
   * @param compareModificationTimes whether to compare modification times.
   * @return true if the two directories are equal
   *
   * @throws IOException if there is an error accessing the filesystem
   */
  public static boolean equalDirs(Configuration conf, Path src, Path dest,
      Optional<PathFilter> filter, boolean compareModificationTimes) throws IOException {
    boolean srcExists = src.getFileSystem(conf).exists(src);
    boolean destExists = dest.getFileSystem(conf).exists(dest);

    if (!srcExists || !destExists) {
      return false;
    }

    Set<FileStatus> srcFileStatuses = getFileStatusesRecursive(conf, src, filter);
    Set<FileStatus> destFileStatuses = getFileStatusesRecursive(conf, dest, filter);

    Map<String, Long> srcFileSizes = null;
    Map<String, Long> destFileSizes = null;

    try {
      srcFileSizes = getRelPathToSizes(src, srcFileStatuses);
      destFileSizes = getRelPathToSizes(dest, destFileStatuses);
    } catch (ArgumentException e) {
      throw new IOException("Invalid file statuses!", e);
    }

    long srcSize = totalSize(srcFileSizes);
    long destSize = totalSize(destFileSizes);

    // Size check is sort of redundant, but is a quick one to show.
    LOG.debug("Size of " + src + " is " + srcSize);
    LOG.debug("Size of " + dest + " is " + destSize);

    if (srcSize != destSize) {
      LOG.debug(String.format("Size of %s and %s do not match!", src, dest));
      return false;
    }

    if (srcFileSizes.size() != destFileSizes.size()) {
      LOG.warn(String.format("Number of files in %s (%d) and %s (%d) " + "do not match!", src,
          srcFileSizes.size(), dest, destFileSizes.size()));
      return false;
    }

    for (String file : srcFileSizes.keySet()) {
      if (!destFileSizes.containsKey(file)) {
        LOG.warn(String.format("%s missing from %s!", file, dest));
        return false;
      }
      if (!srcFileSizes.get(file).equals(destFileSizes.get(file))) {
        LOG.warn(String.format("Size mismatch between %s (%d) in %s " + "and %s (%d) in %s", file,
            srcFileSizes.get(file), src, file, destFileSizes.get(file), dest));
        return false;
      }
    }

    if (compareModificationTimes) {
      Map<String, Long> srcFileModificationTimes = null;
      Map<String, Long> destFileModificationTimes = null;

      try {
        srcFileModificationTimes = getRelativePathToModificationTime(src, srcFileStatuses);
        destFileModificationTimes = getRelativePathToModificationTime(dest, destFileStatuses);
      } catch (ArgumentException e) {
        throw new IOException("Invalid file statuses!", e);
      }

      for (String file : srcFileModificationTimes.keySet()) {
        if (!srcFileModificationTimes.get(file).equals(destFileModificationTimes.get(file))) {
          LOG.warn(String.format(
              "Modification time mismatch between " + "%s (%d) in %s and %s (%d) in %s", file,
              srcFileModificationTimes.get(file), src, file, destFileModificationTimes.get(file),
              dest));
          return false;
        }
      }
    }

    LOG.debug(String.format("%s and %s are the same", src, dest));
    return true;
  }

  /**
   * Set the file modification times for the files on the destination to be the same as the
   * modification times for the file on the source.
   *
   * @param conf configuration object
   * @param src source directory
   * @param dest destination directory
   * @param filter a filter for excluding some files from modification
   *
   * @throws IOException if there's an error
   */
  public static void syncModificationTimes(Configuration conf, Path src, Path dest,
      Optional<PathFilter> filter) throws IOException {
    Set<FileStatus> srcFileStatuses = getFileStatusesRecursive(conf, src, filter);

    Map<String, Long> srcFileModificationTimes = null;

    try {
      srcFileModificationTimes = getRelativePathToModificationTime(src, srcFileStatuses);
    } catch (ArgumentException e) {
      throw new IOException("Invalid file statuses!", e);
    }

    FileSystem destFs = dest.getFileSystem(conf);

    for (String file : srcFileModificationTimes.keySet()) {
      destFs.setTimes(new Path(dest, file), srcFileModificationTimes.get(file), -1);
    }
  }

  /**
   * Moves the directory from the src to dest, creating the parent directory for the dest if one
   * does not exist.
   *
   * @param conf configuration object
   * @param src source directory
   * @param dest destination directory
   * @throws IOException if there's an error moving the directory
   */
  public static void moveDir(Configuration conf, Path src, Path dest) throws IOException {
    FileSystem srcFs = FileSystem.get(src.toUri(), conf);
    FileSystem destFs = FileSystem.get(dest.toUri(), conf);
    if (!srcFs.getUri().equals(destFs.getUri())) {
      throw new IOException("Source and destination filesystems " + "are different! src: "
          + srcFs.getUri() + " dest: " + destFs.getUri());
    }

    Path destPathParent = dest.getParent();
    if (destFs.exists(destPathParent)) {
      if (!destFs.isDirectory(destPathParent)) {
        throw new IOException("File exists instead of destination " + destPathParent);
      } else {
        LOG.debug("Parent directory exists: " + destPathParent);
      }
    } else {
      destFs.mkdirs(destPathParent);
    }
    boolean successful = srcFs.rename(src, dest);
    if (!successful) {
      throw new IOException("Error while moving from " + src + " to " + dest);
    }
  }

  /**
   * Checks to see if a directory exists.
   *
   * @param conf configuration object
   * @param path the path to check
   * @return true if the path specifies a directory that exists
   *
   * @throws IOException if there's an error accessing the filesystem
   */
  public static boolean dirExists(Configuration conf, Path path) throws IOException {
    FileSystem fs = FileSystem.get(path.toUri(), conf);
    return fs.exists(path) && fs.isDirectory(path);
  }

  /**
   * Delete the specified directory, using the trash as available.
   *
   * @param conf configuration object
   * @param path path to delete
   *
   * @throws IOException if there's an error deleting the directory.
   */
  public static void deleteDirectory(Configuration conf, Path path) throws IOException {

    Trash trash = new Trash(path.getFileSystem(conf), conf);
    try {
      if (!trash.isEnabled()) {
        LOG.debug("Trash is not enabled for " + path + " so deleting instead");
        FileSystem fs = path.getFileSystem(conf);
        fs.delete(path, true);
      } else {
        boolean removed = trash.moveToTrash(path);
        if (removed) {
          LOG.debug("Moved to trash: " + path);
        } else {
          LOG.error("Item already in trash: " + path);
        }
      }
    } catch (FileNotFoundException e) {
      LOG.debug("Attempting to delete non-existent directory " + path);
      return;
    }
  }

  /**
   * Checks to see if one directory is a subdirectory of another.
   *
   * @param p1 directory
   * @param p2 potential subdirectory
   * @return true if p2 is a subdirectory of p1
   */
  public static boolean isSubDirectory(Path p1, Path p2) {
    URI relativizedUri = p1.toUri().relativize(p2.toUri());
    return !relativizedUri.equals(p2.toUri());
  }

  /**
   * Replace a directory with another directory.
   *
   * @param conf configuration object
   * @param src source directory
   * @param dest destination directory
   *
   * @throws IOException if there's an error with the filesystem
   */
  public static void replaceDirectory(Configuration conf, Path src, Path dest) throws IOException {
    FileSystem fs = dest.getFileSystem(conf);
    if (fs.exists(dest)) {
      LOG.debug("Removing " + dest + " since it exists");
      deleteDirectory(conf, dest);
    }
    LOG.debug("Renaming " + src + " to " + dest);
    fs.rename(src, dest);
  }

  /**
   * Returns true if the both files have checksums and they match. Returns false if checksums exist
   * but they do not match. Returns empty if either file does not have a checksum.
   *
   * @param conf configuration use to create the FileSystems
   * @param srcFile source file
   * @param destFile destination file
   * @throws IOException if there is an error getting the checksum for the specified files
   */
  public static Optional<Boolean> checksumsMatch(Configuration conf, Path srcFile, Path destFile)
      throws IOException {
    FileSystem srcFs = srcFile.getFileSystem(conf);
    FileChecksum srcChecksum = srcFs.getFileChecksum(srcFile);

    FileSystem destFs = destFile.getFileSystem(conf);
    FileChecksum destChecksum = destFs.getFileChecksum(destFile);

    if (srcChecksum == null || destChecksum == null) {
      // If either filesystem does not support checksums
      return Optional.empty();
    } else {
      return Optional.of(Boolean.valueOf(srcChecksum.equals(destChecksum)));
    }
  }
}
