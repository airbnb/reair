package com.airbnb.reair.batch.hive;

import com.airbnb.reair.common.DistCpException;
import com.airbnb.reair.common.HiveMetastoreClient;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.incremental.DirectoryCopier;
import com.airbnb.reair.incremental.ReplicationUtils;
import com.airbnb.reair.incremental.RunInfo;
import com.airbnb.reair.incremental.configuration.Cluster;
import com.airbnb.reair.incremental.configuration.ClusterFactory;
import com.airbnb.reair.incremental.configuration.ConfigurationException;
import com.airbnb.reair.incremental.configuration.DestinationObjectFactory;
import com.airbnb.reair.incremental.configuration.ObjectConflictHandler;
import com.airbnb.reair.incremental.primitives.CopyPartitionTask;
import com.airbnb.reair.incremental.primitives.CopyPartitionedTableTask;
import com.airbnb.reair.incremental.primitives.CopyUnpartitionedTableTask;
import com.airbnb.reair.incremental.primitives.DropPartitionTask;
import com.airbnb.reair.incremental.primitives.DropTableTask;
import com.airbnb.reair.incremental.primitives.TaskEstimate;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Optional;

/**
 * Stage 3 mapper to commit metadata changes.
 *
 * <p>Input of the Stage 3 job is Stage 1 job output, which is a list of actions to take for each
 * table / partition.
 */
public class Stage3CommitChangeMapper extends Mapper<LongWritable, Text, Text, Text> {
  private static final Log LOG = LogFactory.getLog(Stage3CommitChangeMapper.class);
  private static final DestinationObjectFactory DESTINATION_OBJECT_FACTORY =
      new DestinationObjectFactory();

  private Configuration conf;
  private HiveMetastoreClient srcClient;
  private HiveMetastoreClient dstClient;
  private Cluster srcCluster;
  private Cluster dstCluster;
  private DirectoryCopier directoryCopier;
  private ObjectConflictHandler objectConflictHandler = new ObjectConflictHandler();

  protected void setup(Context context) throws IOException, InterruptedException {
    try {
      this.conf = context.getConfiguration();
      ClusterFactory clusterFactory = MetastoreReplUtils.createClusterFactory(conf);

      this.srcCluster = clusterFactory.getSrcCluster();
      this.srcClient = this.srcCluster.getMetastoreClient();

      this.dstCluster = clusterFactory.getDestCluster();
      this.dstClient = this.dstCluster.getMetastoreClient();

      this.directoryCopier = clusterFactory.getDirectoryCopier();
    } catch (HiveMetastoreException | ConfigurationException e) {
      throw new IOException(e);
    }
  }

  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
    try {
      Pair<TaskEstimate, HiveObjectSpec> input =
          MetastoreReplicationJob.deseralizeJobResult(value.toString());
      TaskEstimate estimate = input.getLeft();
      HiveObjectSpec spec = input.getRight();
      RunInfo status = null;

      LOG.info(String.format("Working on %s with estimate %s", spec, estimate));
      switch (estimate.getTaskType()) {
        case COPY_PARTITION:
          CopyPartitionTask copyPartitionTask = new CopyPartitionTask(
              conf,
              DESTINATION_OBJECT_FACTORY,
              objectConflictHandler,
              srcCluster,
              dstCluster,
              spec,
              Optional.<Path>empty(),
              Optional.<Path>empty(),
              directoryCopier,
              false);
          status = copyPartitionTask.runTask();
          context.write(value, new Text(status.getRunStatus().toString()));
          break;

        case COPY_PARTITIONED_TABLE:
          CopyPartitionedTableTask copyPartitionedTableTaskJob = new CopyPartitionedTableTask(
              conf,
              DESTINATION_OBJECT_FACTORY,
              objectConflictHandler,
              srcCluster,
              dstCluster,
              spec,
              Optional.<Path>empty());
          status = copyPartitionedTableTaskJob.runTask();
          context.write(value, new Text(status.getRunStatus().toString()));
          break;

        case COPY_UNPARTITIONED_TABLE:
          CopyUnpartitionedTableTask copyUnpartitionedTableTask = new CopyUnpartitionedTableTask(
              conf,
              DESTINATION_OBJECT_FACTORY,
              objectConflictHandler,
              srcCluster,
              dstCluster,
              spec,
              Optional.<Path>empty(),
              directoryCopier,
              false);
          status = copyUnpartitionedTableTask.runTask();
          context.write(value, new Text(status.getRunStatus().toString()));
          break;

        case DROP_PARTITION:
          Partition dstPart = dstClient.getPartition(
                                  spec.getDbName(),
                                  spec.getTableName(),
                                  spec.getPartitionName());
          if (dstPart == null) {
            context.write(value, new Text(RunInfo.RunStatus.SUCCESSFUL.toString()));
            break;
          }

          DropPartitionTask dropPartitionTask = new DropPartitionTask(srcCluster,
              dstCluster,
              spec,
              ReplicationUtils.getTldt(dstPart));

          status = dropPartitionTask.runTask();
          context.write(value, new Text(status.getRunStatus().toString()));
          break;

        case DROP_TABLE:
          Table dstTable = dstClient.getTable(spec.getDbName(), spec.getTableName());
          if (dstTable == null) {
            context.write(value, new Text(RunInfo.RunStatus.SUCCESSFUL.toString()));
            break;
          }

          DropTableTask dropTableTask = new DropTableTask(srcCluster,
              dstCluster,
              spec,
              ReplicationUtils.getTldt(dstTable));
          status = dropTableTask.runTask();
          context.write(value, new Text(status.getRunStatus().toString()));
          break;

        default:
          break;
      }
    } catch (HiveMetastoreException | DistCpException | ConfigurationException e) {
      LOG.error(String.format("Got exception while processing %s", value.toString()), e);
      context.write(value, new Text(RunInfo.RunStatus.FAILED.toString()));
    }
  }

  protected void cleanup(Context context) throws IOException, InterruptedException {
    this.srcClient.close();
    this.dstClient.close();
  }
}
