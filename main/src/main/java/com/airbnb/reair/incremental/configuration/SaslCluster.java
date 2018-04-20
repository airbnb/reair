package com.airbnb.reair.incremental.configuration;

import com.airbnb.reair.common.HiveMetastoreClient;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.SaslHiveMetastoreClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;

import java.net.URI;

public class SaslCluster implements Cluster {

  private final Configuration configuration;
  private final String name;
  private final URI metastoreUri;
  private final Path hdfsRoot;
  private final Path tmpDir;
  private final String metastorePrincipal;
  private final String tokenSignature;
  private final ThreadLocal<SaslHiveMetastoreClient> metastoreClient;

  /**
   * Constructor with specific values.
   *
   * @param configuration job configuration
   * @param name string to use for identifying this cluster
   * @param metastoreUri uri of the metastore Thrift server
   * @param hdfsRoot the path for the root HDFS directory
   * @param tmpDir the path for the temporary HDFS directory (should be under root)
   * @param metastorePrincipal the service principal for the metastore thrift server
   * @param tokenSignature string used to store delegation token
   */
  public SaslCluster(
      Configuration configuration,
      String name,
      URI metastoreUri,
      Path hdfsRoot,
      Path tmpDir,
      String metastorePrincipal,
      String tokenSignature) {
    this.configuration = configuration;
    this.name = name;
    this.metastoreUri = metastoreUri;
    this.hdfsRoot = hdfsRoot;
    this.tmpDir = tmpDir;
    this.metastorePrincipal = metastorePrincipal;
    this.tokenSignature = tokenSignature;
    this.metastoreClient = new ThreadLocal<SaslHiveMetastoreClient>();
  }

  /**
   * Get a cached ThreadLocal metastore client.
   */
  @Override
  public HiveMetastoreClient getMetastoreClient() throws HiveMetastoreException {
    SaslHiveMetastoreClient result = this.metastoreClient.get();
    if (result == null) {
      IMetaStoreClient metastoreClient = SaslClusterUtils.getMetastoreClient(
              metastoreUri, tokenSignature, metastorePrincipal, configuration);
      result = new SaslHiveMetastoreClient(metastoreClient);
      this.metastoreClient.set(result);
    }
    return result;
  }

  public Path getFsRoot() {
    return hdfsRoot;
  }

  public Path getTmpDir() {
    return tmpDir;
  }

  public String getName() {
    return name;
  }
}
