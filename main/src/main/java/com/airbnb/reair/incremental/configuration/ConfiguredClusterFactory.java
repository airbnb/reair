package com.airbnb.reair.incremental.configuration;

import com.airbnb.reair.incremental.deploy.ConfigurationKeys;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class ConfiguredClusterFactory extends AbstractClusterFactory {

  @Override
  public Cluster getDestCluster() throws ConfigurationException {
    Configuration conf = getConf();

    String destClusterName = conf.get(
        ConfigurationKeys.DEST_CLUSTER_NAME);
    String destMetastoreUrlString = conf.get(
        ConfigurationKeys.DEST_CLUSTER_METASTORE_URL);
    URI destMetastoreUrl = makeUri(destMetastoreUrlString);
    String destHdfsRoot = conf.get(
        ConfigurationKeys.DEST_HDFS_ROOT);
    String destHdfsTmp = conf.get(
        ConfigurationKeys.DEST_HDFS_TMP);
    return new HardCodedCluster(
        destClusterName,
        destMetastoreUrl.getHost(),
        destMetastoreUrl.getPort(),
        null,
        null,
        new Path(destHdfsRoot),
        new Path(destHdfsTmp));
  }

  @Override
  public Cluster getSrcCluster() throws ConfigurationException {
    Configuration conf = getConf();

    String srcClusterName = conf.get(
        ConfigurationKeys.SRC_CLUSTER_NAME);
    String srcMetastoreUrlString = conf.get(
        ConfigurationKeys.SRC_CLUSTER_METASTORE_URL);
    URI srcMetastoreUrl = makeUri(srcMetastoreUrlString);
    String srcHdfsRoot = conf.get(
        ConfigurationKeys.SRC_HDFS_ROOT);
    String srcHdfsTmp = conf.get(
        ConfigurationKeys.SRC_HDFS_TMP);
    return new HardCodedCluster(
        srcClusterName,
        srcMetastoreUrl.getHost(),
        srcMetastoreUrl.getPort(),
        null,
        null,
        new Path(srcHdfsRoot),
        new Path(srcHdfsTmp));
  }
}
