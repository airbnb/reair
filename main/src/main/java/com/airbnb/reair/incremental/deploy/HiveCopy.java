package com.airbnb.reair.incremental.deploy;

import com.airbnb.reair.common.ArgumentException;
import com.airbnb.reair.common.CliUtils;
import com.airbnb.reair.common.HiveMetastoreClient;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.incremental.DirectoryCopier;
import com.airbnb.reair.incremental.ReplicationUtils;
import com.airbnb.reair.incremental.RunInfo;
import com.airbnb.reair.incremental.configuration.DestinationObjectFactory;
import com.airbnb.reair.incremental.configuration.HardCodedCluster;
import com.airbnb.reair.incremental.configuration.ObjectConflictHandler;
import com.airbnb.reair.incremental.primitives.CopyPartitionTask;
import com.airbnb.reair.incremental.primitives.CopyPartitionedTableTask;
import com.airbnb.reair.incremental.primitives.CopyUnpartitionedTableTask;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;

import java.util.Optional;

public class HiveCopy {

  private static final Log LOG = LogFactory.getLog(HiveCopy.class);

  /**
   * Main entry for copy utility. Warning suppression needed for the OptionBuilder API
   *
   * @param args array of command line arguments
   * @return 0 if copy was successful, non-zero otherwise
   *
   * @throws Exception if there is an error with the copy
   */
  @SuppressWarnings("static-access")
  public static int main(String[] args) throws Exception {
    Options options = new Options();
    options.addOption(OptionBuilder.withLongOpt("config-file")
        .withDescription("Path to the XML configuration file").hasArg().withArgName("PATH")
        .create());
    options.addOption(OptionBuilder.withLongOpt("op")
        .withDescription("name of operation to perform").hasArg().withArgName("OP").create());
    options.addOption(OptionBuilder.withLongOpt("db")
        .withDescription(
            "Hive DB where the table or partition that " + "you want to replicate resides")
        .hasArg().withArgName("DB").create());
    options.addOption(OptionBuilder.withLongOpt("table")
        .withDescription("Hive table to replicate or if a partition "
            + "is specified, the source table for the partition")
        .hasArg().withArgName("TABLE").create());
    options.addOption(OptionBuilder.withLongOpt("partition")
        .withDescription("Hive partition to replicate").hasArg().withArgName("PARTITION").create());
    options.addOption(OptionBuilder.withLongOpt("partition")
        .withDescription("Hive partition to replicate").hasArg().withArgName("PARTITION").create());
    options.addOption(OptionBuilder.withLongOpt("help").withDescription("Help message").create());

    CommandLineParser parser = new BasicParser();
    CommandLine cl = parser.parse(options, args);

    String configPath = null;
    String dbName = null;
    String tableName = null;
    String partitionName = null;
    String op = null;

    if (cl.hasOption("help")) {
      CliUtils.printHelp("<command>", options);
    }

    if (cl.hasOption("config-file")) {
      configPath = cl.getOptionValue("config-file");
      LOG.info("config-file=" + configPath);
    }


    if (cl.hasOption("db")) {
      dbName = cl.getOptionValue("db");
      LOG.info("db=" + dbName);
    }

    if (cl.hasOption("table")) {
      tableName = cl.getOptionValue("table");
      LOG.info("table=" + tableName);
    }

    if (cl.hasOption("partition")) {
      partitionName = cl.getOptionValue("partition");
      LOG.info("partition=" + partitionName);
    }

    if (cl.hasOption("op")) {
      op = cl.getOptionValue("op");
      LOG.info("op=" + op);
    }

    if (configPath == null) {
      throw new ArgumentException("config path not specified");
    }

    if (dbName == null) {
      throw new ArgumentException("db was not specified!");
    }

    if (tableName == null) {
      throw new ArgumentException("table was not specified!");
    }

    Configuration conf = new Configuration();
    conf.addResource(new Path(configPath));

    String srcName = conf.get("airbnb.hive.cluster.src.name");
    String srcMetastoreHost = conf.get("airbnb.hive.cluster.src.metastore.host");
    int srcMetastorePort = Integer.parseInt(conf.get("airbnb.hive.cluster.src.metastore.port"));
    Path srcHdfsRoot = new Path(conf.get("airbnb.hive.cluster.src.hdfs.root"));
    Path srcTmpDir = new Path(conf.get("airbnb.hive.cluster.src.hdfs.tmp.dir"));

    String destName = conf.get("airbnb.hive.cluster.dest.name");
    String destMetastoreHost = conf.get("airbnb.hive.cluster.dest.metastore.host");
    int destMetastorePort = Integer.parseInt(conf.get("airbnb.hive.cluster.dest.metastore.port"));
    Path destHdfsRoot = new Path(conf.get("airbnb.hive.cluster.dest.hdfs.root"));
    Path destTmpDir = new Path(conf.get("airbnb.hive.cluster.dest.hdfs.tmp.dir"));

    HiveObjectSpec spec = new HiveObjectSpec(dbName, tableName, partitionName);

    LOG.info("srcName=" + srcName);
    LOG.info("srcMetastoreHost=" + srcMetastoreHost);
    LOG.info("srcMetastorePort=" + srcMetastorePort);
    LOG.info("srcHdfsRoot=" + srcHdfsRoot);
    LOG.info("srcTmpDir=" + srcTmpDir);

    LOG.info("destName=" + destName);
    LOG.info("destMetastoreHost=" + destMetastoreHost);
    LOG.info("destMetastorePort=" + destMetastorePort);
    LOG.info("destHdfsRoot=" + destHdfsRoot);
    LOG.info("destTmpDir=" + destTmpDir);
    LOG.info("pool.name=" + conf.get("pool.name"));

    LOG.info("spec=" + spec);

    HardCodedCluster srcCluster = new HardCodedCluster(srcName, srcMetastoreHost, srcMetastorePort,
        null, null, srcHdfsRoot, srcTmpDir);

    HardCodedCluster destCluster = new HardCodedCluster(destName, destMetastoreHost,
        destMetastorePort, null, null, destHdfsRoot, destTmpDir);

    DirectoryCopier directoryCopier = new DirectoryCopier(conf, destTmpDir, true);

    ObjectConflictHandler conflictHandler = new ObjectConflictHandler();
    conflictHandler.setConf(conf);
    DestinationObjectFactory destinationObjectFactory = new DestinationObjectFactory();
    destinationObjectFactory.setConf(conf);

    if ("copy-unpartitioned-table".equals(op)) {
      LOG.info("Copying an unpartitioned table");
      HiveMetastoreClient ms = srcCluster.getMetastoreClient();
      Table srcTable = ms.getTable(spec.getDbName(), spec.getTableName());
      CopyUnpartitionedTableTask job = new CopyUnpartitionedTableTask(conf,
          destinationObjectFactory, conflictHandler, srcCluster, destCluster, spec,
          ReplicationUtils.getLocation(srcTable), directoryCopier, true);
      if (job.runTask().getRunStatus() == RunInfo.RunStatus.SUCCESSFUL) {
        return 0;
      } else {
        return -1;
      }
    } else if ("copy-partitioned-table".equals(op)) {
      LOG.info("Copying a partitioned table");
      HiveMetastoreClient ms = srcCluster.getMetastoreClient();
      Table srcTable = ms.getTable(spec.getDbName(), spec.getTableName());
      CopyPartitionedTableTask job = new CopyPartitionedTableTask(conf, destinationObjectFactory,
          conflictHandler, srcCluster, destCluster, spec, ReplicationUtils.getLocation(srcTable));
      if (job.runTask().getRunStatus() == RunInfo.RunStatus.SUCCESSFUL) {
        return 0;
      } else {
        return -1;
      }
    } else if (op.equals("copy-partition")) {
      LOG.info("Copying a partition");
      HiveMetastoreClient ms = srcCluster.getMetastoreClient();
      Partition srcPartition =
          ms.getPartition(spec.getDbName(), spec.getTableName(), spec.getPartitionName());
      CopyPartitionTask job = new CopyPartitionTask(conf, destinationObjectFactory, conflictHandler,
          srcCluster, destCluster, spec, ReplicationUtils.getLocation(srcPartition),
          Optional.<Path>empty(), new DirectoryCopier(conf, srcCluster.getTmpDir(), true), true);
      if (job.runTask().getRunStatus() == RunInfo.RunStatus.SUCCESSFUL) {
        return 0;
      } else {
        return -1;
      }
    } else {
      throw new RuntimeException("Unhandled op " + op);
    }
  }
}
