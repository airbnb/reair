package com.airbnb.reair.incremental.configuration;

import com.airbnb.reair.incremental.DirectoryCopier;
import com.airbnb.reair.incremental.deploy.ConfigurationKeys;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

public class ConfiguredClusterFactory implements ClusterFactory {

  private Optional<Configuration> optionalConf = Optional.empty();

  public void setConf(Configuration conf) {
    this.optionalConf = Optional.of(conf);
  }

  private static URI makeUri(String thriftUri) throws ConfigurationException {
    try {
      URI uri = new URI(thriftUri);

      if (uri.getPort() <= 0) {
        throw new ConfigurationException("No port specified in "
            + thriftUri);
      }

      if (!"thrift".equals(uri.getScheme())) {
        throw new ConfigurationException("Not a thrift URI; "
            + thriftUri);
      }
      return uri;
    } catch (URISyntaxException e) {
      throw new ConfigurationException(e);
    }
  }

  @Override
  public Cluster getDestCluster() throws ConfigurationException {

    if (!optionalConf.isPresent()) {
      throw new ConfigurationException("Configuration not set!");
    }

    Configuration conf = optionalConf.get();

    String destClusterName = conf.get(
        ConfigurationKeys.DEST_CLUSTER_NAME);
    String destMetastoreUrlString = conf.get(
        ConfigurationKeys.DEST_CLUSTER_METASTORE_URL);
    URI destMetastoreUrl = makeUri(destMetastoreUrlString);
    String destHdfsRoot = conf.get(
        ConfigurationKeys.DEST_HDFS_ROOT);
    String destHdfsTmp = conf.get(
        ConfigurationKeys.DEST_HDFS_TMP);
    // Note that the DEST_CLUSTER_METASTORE_DB_PREFIX is a special config that only exists on the
    // dest cluster (but not on the src cluster).
    String destDbPrefix = conf.get(
        ConfigurationKeys.DEST_CLUSTER_METASTORE_DB_PREFIX);
    return new HardCodedCluster(
        destClusterName,
        destMetastoreUrl.getHost(),
        destMetastoreUrl.getPort(),
        null,
        null,
        new Path(destHdfsRoot),
        new Path(destHdfsTmp),
        destDbPrefix);
  }

  @Override
  public Cluster getSrcCluster() throws ConfigurationException {

    if (!optionalConf.isPresent()) {
      throw new ConfigurationException("Configuration not set!");
    }

    Configuration conf = optionalConf.get();
    // Create the source cluster object
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

  @Override
  public DirectoryCopier getDirectoryCopier() throws ConfigurationException {
    if (!optionalConf.isPresent()) {
      throw new ConfigurationException("Configuration not set!");
    }

    Configuration conf = optionalConf.get();
    String destHdfsTmp = conf.get(
        ConfigurationKeys.DEST_HDFS_TMP);
    return new DirectoryCopier(conf, 
        new Path(destHdfsTmp),
        conf.getBoolean(ConfigurationKeys.SYNC_MODIFIED_TIMES_FOR_FILE_COPY, true));
  }
}
