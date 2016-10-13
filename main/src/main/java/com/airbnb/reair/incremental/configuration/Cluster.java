package com.airbnb.reair.incremental.configuration;

import com.airbnb.reair.common.HiveMetastoreClient;
import com.airbnb.reair.common.HiveMetastoreException;

import org.apache.hadoop.fs.Path;

/**
 * Encapsulates information about a cluster - generally a HDFS, MR, and a Hive metastore that are
 * considered as a unit.
 */
public interface Cluster {
  HiveMetastoreClient getMetastoreClient() throws HiveMetastoreException;

  Path getFsRoot();

  Path getTmpDir();

  String getName();
}
