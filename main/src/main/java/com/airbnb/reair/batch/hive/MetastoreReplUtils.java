package com.airbnb.reair.batch.hive;

import com.airbnb.reair.incremental.configuration.ClusterFactory;
import com.airbnb.reair.incremental.configuration.ConfiguredClusterFactory;
import com.airbnb.reair.incremental.deploy.ConfigurationKeys;

import org.apache.hadoop.conf.Configuration;

/**
 * Utilities class for metastore replication.
 */
public class MetastoreReplUtils {
  private MetastoreReplUtils() {
  }

  /**
   * Static function to create ClusterFactory object based on configuration. For test environment it
   * will create a mock ClusterFactory.
   *
   * @param conf configuration for the cluster
   * @return ClusterFactory implementation
   */
  public static ClusterFactory createClusterFactory(Configuration conf) {
    String clusterFactoryClassName =
        conf.get(ConfigurationKeys.BATCH_JOB_CLUSTER_FACTORY_CLASS);
    if (clusterFactoryClassName != null) {
      ClusterFactory factory = null;
      try {
        factory = (ClusterFactory) Class.forName(clusterFactoryClassName).newInstance();
        factory.setConf(conf);
      } catch (InstantiationException | IllegalAccessException | ClassNotFoundException e) {
        throw new RuntimeException(e);
      }
      return factory;
    } else {
      ConfiguredClusterFactory configuredClusterFactory = new ConfiguredClusterFactory();
      configuredClusterFactory.setConf(conf);
      return configuredClusterFactory;
    }
  }
}
