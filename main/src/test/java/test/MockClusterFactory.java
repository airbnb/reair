package test;

import com.airbnb.reair.incremental.DirectoryCopier;
import com.airbnb.reair.incremental.configuration.Cluster;
import com.airbnb.reair.incremental.configuration.ClusterFactory;
import com.airbnb.reair.incremental.configuration.ConfigurationException;

import org.apache.hadoop.conf.Configuration;

/**
 * Returns static instances of Clusters for testing.
 */
public class MockClusterFactory implements ClusterFactory {
  @Override
  public void setConf(Configuration conf) {
  }

  @Override
  public Configuration getConf() throws ConfigurationException {
    return null;
  }

  @Override
  public Cluster getSrcCluster() {
    return MockClusterTest.srcCluster;
  }

  @Override
  public Cluster getDestCluster() {
    return MockClusterTest.destCluster;
  }

  @Override
  public DirectoryCopier getDirectoryCopier() throws ConfigurationException {
    return MockClusterTest.directoryCopier;
  }
}
