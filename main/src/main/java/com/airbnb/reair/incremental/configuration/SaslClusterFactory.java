package com.airbnb.reair.incremental.configuration;

import com.airbnb.reair.incremental.deploy.ConfigurationKeys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.net.URI;

public class SaslClusterFactory extends AbstractClusterFactory {

  @Override
  public Cluster getDestCluster() throws ConfigurationException {
    Configuration conf = getConf();

    String destClusterName = conf.get(ConfigurationKeys.DEST_CLUSTER_NAME);
    String destMetastoreUrlString = conf.get(ConfigurationKeys.DEST_CLUSTER_METASTORE_URL);
    URI destMetastoreUrl = makeUri(destMetastoreUrlString);
    String destHdfsRoot = conf.get(ConfigurationKeys.DEST_HDFS_ROOT);
    String destHdfsTmp = conf.get(ConfigurationKeys.DEST_HDFS_TMP);
    String metastorePrincipal = conf.get(ConfigurationKeys.DEST_METASTORE_PRINCIPAL);

    return new SaslCluster(
      conf,
      destClusterName,
      destMetastoreUrl,
      new Path(destHdfsRoot),
      new Path(destHdfsTmp),
      metastorePrincipal,
      SaslClusterUtils.REAIR_KEY_TOKEN_SIGNATURE_DEST);
  }

  @Override
  public Cluster getSrcCluster() throws ConfigurationException {
    Configuration conf = getConf();

    String srcClusterName = conf.get(ConfigurationKeys.SRC_CLUSTER_NAME);
    String srcMetastoreUrlString = conf.get(ConfigurationKeys.SRC_CLUSTER_METASTORE_URL);
    URI srcMetastoreUrl = makeUri(srcMetastoreUrlString);
    String srcHdfsRoot = conf.get(ConfigurationKeys.SRC_HDFS_ROOT);
    String srcHdfsTmp = conf.get(ConfigurationKeys.SRC_HDFS_TMP);
    String metastorePrincipal = conf.get(ConfigurationKeys.SRC_METASTORE_PRINCIPAL);

    return new SaslCluster(
      conf,
      srcClusterName,
      srcMetastoreUrl,
      new Path(srcHdfsRoot),
      new Path(srcHdfsTmp),
      metastorePrincipal,
      SaslClusterUtils.REAIR_KEY_TOKEN_SIGNATURE_SRC);
  }
}
