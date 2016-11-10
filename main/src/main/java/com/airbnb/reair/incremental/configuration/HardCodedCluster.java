package com.airbnb.reair.incremental.configuration;

import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.ThriftHiveMetastoreClient;

import org.apache.hadoop.fs.Path;

/**
 * A cluster defined with hard coded values, typically derived from the configuration.
 */
public class HardCodedCluster implements Cluster {

  private String name;
  private String metastoreHost;
  private int metastorePort;
  private String jobtrackerHost;
  private String jobtrackerPort;
  private Path hdfsRoot;
  private Path tmpDir;
  private ThreadLocal<ThriftHiveMetastoreClient> metastoreClient;

  /**
   * Constructor with specific values.
   *
   * @param name string to use for identifying this cluster
   * @param metastoreHost hostname of the metastore Thrift server
   * @param metastorePort port of the metastore Thrift server
   * @param jobtrackerHost hostname of the job tracker
   * @param jobtrackerPort port of the job tracker
   * @param hdfsRoot the path for the root HDFS directory
   * @param tmpDir the path for the temporary HDFS directory (should be under root)
   */
  public HardCodedCluster(
      String name,
      String metastoreHost,
      int metastorePort,
      String jobtrackerHost,
      String jobtrackerPort,
      Path hdfsRoot,
      Path tmpDir) {
    this.name = name;
    this.metastoreHost = metastoreHost;
    this.metastorePort = metastorePort;
    this.jobtrackerHost = jobtrackerHost;
    this.jobtrackerPort = jobtrackerPort;
    this.hdfsRoot = hdfsRoot;
    this.tmpDir = tmpDir;
    this.metastoreClient = new ThreadLocal<ThriftHiveMetastoreClient>();
  }

  public String getMetastoreHost() {
    return metastoreHost;
  }

  public int getMetastorePort() {
    return metastorePort;
  }

  /**
   * Get a cached ThreadLocal metastore client.
   */
  public ThriftHiveMetastoreClient getMetastoreClient() throws HiveMetastoreException {
    ThriftHiveMetastoreClient result = this.metastoreClient.get();
    if (result == null) {
      result = new ThriftHiveMetastoreClient(getMetastoreHost(), getMetastorePort());
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
