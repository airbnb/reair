package com.airbnb.reair.incremental.configuration;

import com.airbnb.reair.common.HiveMetastoreClient;
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
    private HiveMetastoreClient client;

    /**
     * Constructor with specific values.
     *
     * @param name           string to use for identifying this cluster
     * @param metastoreHost  hostname of the metastore Thrift server
     * @param metastorePort  port of the metastore Thrift server
     * @param jobtrackerHost hostname of the job tracker
     * @param jobtrackerPort port of the job tracker
     * @param hdfsRoot       the path for the root HDFS directory
     * @param tmpDir         the path for the temporary HDFS directory (should be under root)
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
    }

    public String getMetastoreHost() {
        return metastoreHost;
    }

    public int getMetastorePort() {
        return metastorePort;
    }


    @Override
    public HiveMetastoreClient getMetastoreClient() throws HiveMetastoreException {
        if (client == null) {
            client = new ThriftHiveMetastoreClient(getMetastoreHost(), getMetastorePort());
            return client;
        }
        return client;
    }


    public HiveMetastoreClient reconnectMetastoreClient() throws HiveMetastoreException {
        client = new ThriftHiveMetastoreClient(getMetastoreHost(), getMetastorePort());
        return client;
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
