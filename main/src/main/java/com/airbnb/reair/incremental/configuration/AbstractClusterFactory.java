package com.airbnb.reair.incremental.configuration;

import com.airbnb.reair.incremental.DirectoryCopier;
import com.airbnb.reair.incremental.deploy.ConfigurationKeys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Optional;

public abstract class AbstractClusterFactory implements ClusterFactory {

  private Optional<Configuration> optionalConf = Optional.empty();

  @Override
  public void setConf(Configuration conf) {
    this.optionalConf = Optional.of(conf);
  }

  @Override
  public Configuration getConf() throws ConfigurationException {
    if (!optionalConf.isPresent()) {
      throw new ConfigurationException("Configuration not set!");
    }
    return optionalConf.get();
  }

  @Override
  public DirectoryCopier getDirectoryCopier() throws ConfigurationException {
    Configuration conf = getConf();
    String destHdfsTmp = conf.get(
            ConfigurationKeys.DEST_HDFS_TMP);
    return new DirectoryCopier(conf,
            new Path(destHdfsTmp),
            conf.getBoolean(ConfigurationKeys.SYNC_MODIFIED_TIMES_FOR_FILE_COPY, true));
  }

  protected static URI makeUri(String thriftUri) throws ConfigurationException {
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
}
