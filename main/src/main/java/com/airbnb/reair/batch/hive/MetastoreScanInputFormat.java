package com.airbnb.reair.batch.hive;

import com.google.common.base.Function;
import com.google.common.collect.Lists;

import com.airbnb.reair.common.HiveMetastoreClient;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.incremental.configuration.Cluster;
import com.airbnb.reair.incremental.configuration.ClusterFactory;
import com.airbnb.reair.incremental.configuration.ConfigurationException;
import com.airbnb.reair.incremental.deploy.ConfigurationKeys;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import javax.annotation.Nullable;

/**
 * Input format that scans the metastore and generates splits containing tables.
 */
public class MetastoreScanInputFormat extends FileInputFormat<Text, Text> {
  private static final Log LOG = LogFactory.getLog(MetastoreScanInputFormat.class);
  private static final int NUMBER_OF_THREADS = 16;

  @Override
  public RecordReader<Text, Text> createRecordReader(
      InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext)
  throws IOException, InterruptedException {
    return new TableRecordReader();
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    // split into pieces, fetching the splits in parallel
    ExecutorService executor = Executors.newCachedThreadPool();
    Cluster srcCluster = null;
    HiveMetastoreClient srcClient = null;
    List<String> allTables = new ArrayList<>();
    final int numberOfMappers = context.getConfiguration()
            .getInt(ConfigurationKeys.BATCH_JOB_METASTORE_PARALLELISM, 150);

    try {
      ClusterFactory clusterFactory =
          MetastoreReplUtils.createClusterFactory(context.getConfiguration());
      srcCluster = clusterFactory.getSrcCluster();
      srcClient = srcCluster.getMetastoreClient();
    } catch (ConfigurationException | HiveMetastoreException e) {
      throw new IOException("Invalid metastore host name.", e);
    }

    try {
      List<String> databases = srcClient.getAllDatabases();
      LOG.info("Total dbs: " + databases.size());

      List<Future<List<String>>> splitfutures = new ArrayList<>();

      final int dbPerThread = Math.max(databases.size() / NUMBER_OF_THREADS, 1);

      for (List<String> range : Lists.partition(databases, dbPerThread)) {
        // for each range, pick a live owner and ask it to compute bite-sized splits
        splitfutures.add(executor.submit(
              new SplitCallable(range, srcCluster)));
      }

      // wait until we have all the results back
      for (Future<List<String>> futureInputSplits : splitfutures) {
        try {
          allTables.addAll(futureInputSplits.get());
        } catch (Exception e) {
          throw new IOException("Could not get input splits", e);
        }
      }

      LOG.info(String.format("Total tables: %d", allTables.size()));

    } catch (HiveMetastoreException e) {
      LOG.error(e.getMessage());
      throw new IOException(e);
    } finally {
      executor.shutdownNow();
      srcClient.close();
    }

    assert allTables.size() > 0;
    Collections.shuffle(allTables, new Random(System.nanoTime()));

    final int tablesPerSplit = Math.max(allTables.size() / numberOfMappers, 1);

    return Lists.transform(
        Lists.partition(allTables, tablesPerSplit),
        new Function<List<String>, InputSplit>() {
          @Override
          public InputSplit apply(@Nullable List<String> tables) {
            return new HiveTablesInputSplit(tables);
          }
        });
  }

  /**
   * Get list of tables from databases in parallel. Each thread will work on database candidates
   * and generate list of table names in those databases.
   */
  class SplitCallable implements Callable<List<String>> {
    private final Cluster cluster;
    private final List<String> candidates;


    public SplitCallable(List<String> candidates, Cluster cluster) {
      this.candidates = candidates;
      this.cluster = cluster;
    }

    public List<String> call() throws Exception {
      ArrayList<String> tables = new ArrayList<>();
      HiveMetastoreClient client = cluster.getMetastoreClient();
      for (final String db: candidates) {
        tables.addAll(Lists.transform(client.getAllTables(db), new Function<String, String>() {
          @Override
          public String apply(String str) {
            return db + ":" + str;
          }
        }));
      }

      LOG.info("Thread " + Thread.currentThread().getId() + ":processed "
          + candidates.size() + " dbs. Produced " + tables.size() + " tables.");

      return tables;
    }
  }
}
