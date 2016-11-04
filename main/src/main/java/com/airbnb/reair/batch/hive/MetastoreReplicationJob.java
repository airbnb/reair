package com.airbnb.reair.batch.hive;

import com.google.common.collect.ImmutableList;

import com.airbnb.reair.batch.template.TemplateRenderException;
import com.airbnb.reair.batch.template.VelocityUtils;
import com.airbnb.reair.common.FsUtils;
import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.incremental.ReplicationUtils;
import com.airbnb.reair.incremental.configuration.ConfigurationException;
import com.airbnb.reair.incremental.deploy.ConfigurationKeys;
import com.airbnb.reair.incremental.primitives.TaskEstimate;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsShell;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.velocity.VelocityContext;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.Calendar;
import java.util.List;
import java.util.Optional;
import java.util.TimeZone;
import java.util.UUID;

public class MetastoreReplicationJob extends Configured implements Tool {
  private static final Log LOG = LogFactory.getLog(MetastoreReplicationJob.class);

  public static final String USAGE_COMMAND_STR = "Usage: hadoop jar <jar name> "
      + MetastoreReplicationJob.class.getName();

  // Context for rendering templates using velocity
  private VelocityContext velocityContext = new VelocityContext();

  // After each job completes, we'll output Hive commands to the screen that can be used to view
  // debug data. These are the templates for those commands.
  private static final String STEP1_HQL_TEMPLATE = "step1_log.hql.vm";
  private static final String STEP2_HQL_TEMPLATE = "step2_log.hql.vm";
  private static final String STEP3_HQL_TEMPLATE = "step3_log.hql.vm";


  /**
   * Serialize TaskEstimate and HiveObjectSpec into a String. The String is passed between MR jobs.
   *
   * @param estimate TaskEstimate object
   * @param spec     HiveObjectSpec
   * @return  serialized output for estimate and spec object
   */
  public static String serializeJobResult(TaskEstimate estimate, HiveObjectSpec spec) {
    return ReplicationUtils.genValue(estimate.getTaskType().name(),
        String.valueOf(estimate.isUpdateMetadata()),
        String.valueOf(estimate.isUpdateData()),
        !estimate.getSrcPath().isPresent() ? null : estimate.getSrcPath().get().toString(),
        !estimate.getDestPath().isPresent() ? null : estimate.getDestPath().get().toString(),
        spec.getDbName(),
        spec.getTableName(),
        spec.getPartitionName());
  }

  /**
   * Deserialize TaskEstimate and HiveObjectSpec from a String.
   *
   * @param result serialized string
   * @return Pair of TaskEstimate and HiveObjectSpec
   */
  public static Pair<TaskEstimate, HiveObjectSpec> deseralizeJobResult(String result) {
    String [] fields = result.split("\t");
    TaskEstimate estimate = new TaskEstimate(TaskEstimate.TaskType.valueOf(fields[0]),
        Boolean.valueOf(fields[1]),
        Boolean.valueOf(fields[2]),
        fields[3].equals("NULL") ? Optional.empty() : Optional.of(new Path(fields[3])),
        fields[4].equals("NULL") ? Optional.empty() : Optional.of(new Path(fields[4])));

    HiveObjectSpec spec = null;
    if (fields[7].equals("NULL")) {
      spec = new HiveObjectSpec(fields[5], fields[6]);
    } else {
      spec = new HiveObjectSpec(fields[5], fields[6], fields[7]);
    }

    return Pair.of(estimate, spec);
  }

  /**
   /**
   * Print usage information to provided OutputStream.
   *
   * @param applicationName Name of application to list in usage.
   * @param options Command-line options to be part of usage.
   * @param out OutputStream to which to write the usage information.
   */
  public static void printUsage(String applicationName, Options options, OutputStream out) {
    PrintWriter writer = new PrintWriter(out);
    HelpFormatter usageFormatter = new HelpFormatter();
    usageFormatter.printUsage(writer, 80, applicationName, options);
    writer.flush();
  }

  /**
   * Run batch replication of the Hive metastore.
   *  1. Parse input arguments.
   *  2. Run three MR jobs in sequence.
   *
   * @param args command arguments
   * @return 1 failed
   *         0 succeeded.
   *
   * @throws Exception  InterruptedException,
   *                    IOException,
   *                    ClassNotFoundException,
   *                    TemplateRenderException
   */
  @SuppressWarnings("static-access")
  public int run(String[] args) throws Exception {
    Options options = new Options();

    options.addOption(OptionBuilder.withLongOpt("config-files")
        .withDescription(
            "Comma separated list of paths to configuration files")
        .hasArg()
        .withArgName("PATH")
        .create());

    options.addOption(OptionBuilder.withLongOpt("step")
        .withDescription("Run a specific step")
        .hasArg()
        .withArgName("ST")
        .create());

    options.addOption(OptionBuilder.withLongOpt("override-input")
        .withDescription("Input override for step")
        .hasArg()
        .withArgName("OI")
        .create());

    options.addOption(OptionBuilder.withLongOpt("table-list")
        .withDescription("File containing a list of tables to copy")
        .hasArg()
        .withArgName("PATH")
        .create());


    CommandLineParser parser = new BasicParser();
    CommandLine cl = null;

    try {
      cl = parser.parse(options, args);
    } catch (ParseException e) {
      System.err.println("Encountered exception while parsing using GnuParser:\n" + e.getMessage());
      printUsage(USAGE_COMMAND_STR, options, System.out);
      System.out.println();
      ToolRunner.printGenericCommandUsage(System.err);
      return 1;
    }

    String configPaths = null;

    if (cl.hasOption("config-files")) {
      configPaths = cl.getOptionValue("config-files");
      LOG.info("configPaths=" + configPaths);

      // load configure and merge with job conf
      Configuration conf = new Configuration();

      if (configPaths != null) {
        for (String configPath : configPaths.split(",")) {
          conf.addResource(new Path(configPath));
        }
      }

      mergeConfiguration(conf, this.getConf());
    } else {
      LOG.warn("Configuration files not specified. Running unit test?");
    }

    if (this.getConf().getBoolean(MRJobConfig.MAP_SPECULATIVE, true)) {
      throw new ConfigurationException(String.format("Speculative execution must be disabled "
          + "for mappers! Please set %s appropriately.", MRJobConfig.MAP_SPECULATIVE));
    }
    if (this.getConf().getBoolean(MRJobConfig.REDUCE_SPECULATIVE, true)) {
      throw new ConfigurationException(String.format("Speculative execution must be disabled "
          + "for reducers! Please set %s appropriately.", MRJobConfig.REDUCE_SPECULATIVE));
    }
    Optional<Path> localTableListFile = Optional.empty();
    if (cl.hasOption("table-list")) {
      localTableListFile = Optional.of(new Path(cl.getOptionValue("table-list")));
    }

    int step = -1;
    if (cl.hasOption("step")) {
      step = Integer.valueOf(cl.getOptionValue("step"));
    }

    String finalOutput = this.getConf().get(ConfigurationKeys.BATCH_JOB_OUTPUT_DIR);
    if (finalOutput == null) {
      System.err.println(
          ConfigurationKeys.BATCH_JOB_OUTPUT_DIR + " is required in configuration file.");
      return 1;
    }

    Path outputParent = new Path(finalOutput);
    Path step1Out = new Path(outputParent, "step1output");
    Path step2Out = new Path(outputParent, "step2output");
    Path step3Out = new Path(outputParent, "step3output");

    Calendar calendar = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    String jobStartTime = String.format("%tY-%<tm-%<tdT%<tH_%<tM_%<tS",
        calendar);

    velocityContext.put("job_start_time", jobStartTime);
    velocityContext.put("step1_output_directory", step1Out.toString());
    velocityContext.put("step2_output_directory", step2Out.toString());
    velocityContext.put("step3_output_directory", step3Out.toString());

    Optional<Path> tableListFileOnHdfs = Optional.empty();
    if (localTableListFile.isPresent()) {
      // Create a temporary directory on HDFS and copy our table list to that directory so that it
      // can be read by mappers in the HDFS job.
      Path tableFilePath = localTableListFile.get();
      Path tmpDir = createTempDirectory(getConf());
      tableListFileOnHdfs = Optional.of(new Path(tmpDir, tableFilePath.getName()));
      LOG.info(String.format("Copying %s to temporary directory %s",
          tableFilePath,
          tableListFileOnHdfs.get()));
      copyFile(localTableListFile.get(), tableListFileOnHdfs.get());
      LOG.info(String.format("Copied %s to temporary directory %s",
          tableFilePath,
          tableListFileOnHdfs.get()));
    } else {
      LOG.info("List of tables to copy is not specified. Copying all tables instead.");
    }

    if (step == -1) {
      LOG.info("Deleting " + step1Out);
      FsUtils.deleteDirectory(getConf(), step1Out);
      LOG.info("Deleting " + step2Out);
      FsUtils.deleteDirectory(getConf(), step2Out);
      LOG.info("Deleting " + step3Out);
      FsUtils.deleteDirectory(getConf(), step3Out);

      if (runMetastoreCompareJob(tableListFileOnHdfs, step1Out) != 0) {
        return -1;
      }

      if (runHdfsCopyJob(step1Out, step2Out) != 0) {
        return -1;
      }

      if (runCommitChangeJob(step1Out, step3Out) != 0) {
        return -1;
      }

      return 0;
    } else {
      switch (step) {
        case 1:
          LOG.info("Deleting " + step1Out);
          FsUtils.deleteDirectory(this.getConf(), step1Out);

          return this.runMetastoreCompareJob(tableListFileOnHdfs, step1Out);
        case 2:
          LOG.info("Deleting " + step2Out);
          FsUtils.deleteDirectory(getConf(), step2Out);
          if (cl.hasOption("override-input")) {
            step1Out = new Path(cl.getOptionValue("override-input"));
          }

          return this.runHdfsCopyJob(new Path(step1Out, "part*"), step2Out);
        case 3:
          LOG.info("Deleting " + step3Out);
          FsUtils.deleteDirectory(this.getConf(), step3Out);
          if (cl.hasOption("override-input")) {
            step1Out = new Path(cl.getOptionValue("override-input"));
          }

          return this.runCommitChangeJob(new Path(step1Out, "part*"), step3Out);
        default:
          LOG.error("Invalid step specified: " + step);
          return 1;
      }
    }
  }

  private void mergeConfiguration(Configuration inputConfig, Configuration merged) {
    List<String> mergeKeys = ImmutableList.of(ConfigurationKeys.SRC_CLUSTER_NAME,
        ConfigurationKeys.SRC_CLUSTER_METASTORE_URL,
        ConfigurationKeys.SRC_HDFS_ROOT,
        ConfigurationKeys.SRC_HDFS_TMP,
        ConfigurationKeys.DEST_CLUSTER_NAME,
        ConfigurationKeys.DEST_CLUSTER_METASTORE_URL,
        ConfigurationKeys.DEST_HDFS_ROOT,
        ConfigurationKeys.DEST_HDFS_TMP,
        ConfigurationKeys.DEST_CLUSTER_METASTORE_DB_PREFIX,
        ConfigurationKeys.BATCH_JOB_METASTORE_BLACKLIST,
        ConfigurationKeys.BATCH_JOB_CLUSTER_FACTORY_CLASS,
        ConfigurationKeys.BATCH_JOB_OUTPUT_DIR,
        ConfigurationKeys.BATCH_JOB_INPUT_LIST,
        ConfigurationKeys.BATCH_JOB_METASTORE_PARALLELISM,
        ConfigurationKeys.BATCH_JOB_COPY_PARALLELISM,
        ConfigurationKeys.SYNC_MODIFIED_TIMES_FOR_FILE_COPY,
        MRJobConfig.MAP_SPECULATIVE,
        MRJobConfig.REDUCE_SPECULATIVE
        );

    for (String key : mergeKeys) {
      String value = inputConfig.get(key);
      if (value != null) {
        merged.set(key, value);
      }
    }
  }

  private int runMetastoreCompareJob(Path output)
    throws IOException, InterruptedException, ClassNotFoundException {
    Job job = Job.getInstance(this.getConf(), "Stage1: Metastore Compare Job");

    job.setJarByClass(this.getClass());
    job.setInputFormatClass(MetastoreScanInputFormat.class);
    job.setMapperClass(Stage1ProcessTableMapper.class);
    job.setReducerClass(PartitionCompareReducer.class);

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    FileOutputFormat.setOutputPath(job, output);
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

    boolean success = job.waitForCompletion(true);

    return success ? 0 : 1;
  }

  /**
   * Runs the job to scan the metastore for directory locations.
   *
   * @param inputTableListPath the path to the file containing the tables to copy
   * @param outputPath the directory to store the logging output data
   */
  private int runMetastoreCompareJob(Optional<Path> inputTableListPath, Path outputPath)
      throws InterruptedException, IOException, ClassNotFoundException, TemplateRenderException {
    LOG.info("Starting job for step 1...");

    int result;
    if (inputTableListPath.isPresent()) {
      result = runMetastoreCompareJobWithTextInput(inputTableListPath.get(), outputPath);
    } else {
      result = runMetastoreCompareJob(outputPath);
    }

    if (result == 0) {
      LOG.info("Job for step 1 finished successfully! To view logging data, run the following "
          + "commands in Hive: \n\n"
          + VelocityUtils.renderTemplate(STEP1_HQL_TEMPLATE, velocityContext));
    }

    return result;
  }

  private int runMetastoreCompareJobWithTextInput(Path input, Path output)
    throws IOException, InterruptedException, ClassNotFoundException {
    Job job = Job.getInstance(this.getConf(), "Stage1: Metastore Compare Job with Input List");

    job.setJarByClass(this.getClass());
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(Stage1ProcessTableMapperWithTextInput.class);
    job.setReducerClass(PartitionCompareReducer.class);

    FileInputFormat.setInputPaths(job, input);
    FileInputFormat.setMaxInputSplitSize(job,
        this.getConf().getLong(FileInputFormat.SPLIT_MAXSIZE, 60000L));

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    FileOutputFormat.setOutputPath(job, output);
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

    job.setNumReduceTasks(getConf().getInt(
        ConfigurationKeys.BATCH_JOB_METASTORE_PARALLELISM,
        150));


    boolean success = job.waitForCompletion(true);

    return success ? 0 : 1;
  }

  private int runHdfsCopyJob(Path input, Path output)
    throws IOException, InterruptedException, ClassNotFoundException, TemplateRenderException {

    LOG.info("Starting job for step 2...");

    Job job = Job.getInstance(this.getConf(), "Stage 2: HDFS Copy Job");

    job.setJarByClass(this.getClass());
    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(Stage2DirectoryCopyMapper.class);
    job.setReducerClass(Stage2DirectoryCopyReducer.class);

    FileInputFormat.setInputPaths(job, input);
    FileInputFormat.setInputDirRecursive(job, true);
    FileInputFormat.setMaxInputSplitSize(job,
        this.getConf().getLong(FileInputFormat.SPLIT_MAXSIZE, 60000L));

    job.setOutputKeyClass(LongWritable.class);
    job.setOutputValueClass(Text.class);

    FileOutputFormat.setOutputPath(job, output);
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

    job.setNumReduceTasks(getConf().getInt(
        ConfigurationKeys.BATCH_JOB_COPY_PARALLELISM,
        150));

    boolean success = job.waitForCompletion(true);

    if (success) {
      LOG.info("Job for step 2 finished successfully! To view logging data, run the following "
          + "commands in Hive: \n\n"
          + VelocityUtils.renderTemplate(STEP2_HQL_TEMPLATE, velocityContext)
          + "\n");
    }

    return success ? 0 : 1;
  }

  private int runCommitChangeJob(Path input, Path output)
    throws IOException, InterruptedException, ClassNotFoundException, TemplateRenderException {

    LOG.info("Starting job for step 3...");

    Job job = Job.getInstance(this.getConf(), "Stage3: Commit Change Job");

    job.setJarByClass(this.getClass());

    job.setInputFormatClass(TextInputFormat.class);
    job.setMapperClass(Stage3CommitChangeMapper.class);
    job.setNumReduceTasks(0);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    FileInputFormat.setInputPaths(job, input);
    FileInputFormat.setInputDirRecursive(job, true);
    FileInputFormat.setMaxInputSplitSize(job,
        this.getConf().getLong(FileInputFormat.SPLIT_MAXSIZE, 60000L));

    FileOutputFormat.setOutputPath(job, output);
    FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);

    job.setNumReduceTasks(getConf().getInt(
        ConfigurationKeys.BATCH_JOB_METASTORE_PARALLELISM,
        150));

    boolean success = job.waitForCompletion(true);

    if (success) {
      LOG.info("Job for step 3 finished successfully! To view logging data, run the following "
          + "commands in Hive: \n\n"
          + VelocityUtils.renderTemplate(STEP3_HQL_TEMPLATE, velocityContext));
    }
    return success ? 0 : 1;
  }

  /**
   * Main function - invoke ToolRunner.run().
   *
   * @param args program arguments
   *
   * @throws Exception  InterruptedException,
   *                    IOException,
   *                    ClassNotFoundException,
   *                    TemplateRenderException
   */
  public static void main(String[] args) throws Exception {
    int res = ToolRunner.run(new MetastoreReplicationJob(), args);

    System.exit(res);
  }

  public static class Stage1ProcessTableMapper extends Mapper<Text, Text, LongWritable, Text> {
    private TableCompareWorker worker = new TableCompareWorker();

    protected void setup(Context context) throws IOException, InterruptedException {
      try {
        worker.setup(context);
      } catch (ConfigurationException e) {
        throw new IOException("Invalid configuration", e);
      }
    }

    protected void map(Text key, Text value, Context context)
        throws IOException, InterruptedException {
      try {
        for (String result : worker.processTable(key.toString(), value.toString())) {
          context.write(new LongWritable((long)result.hashCode()), new Text(result));
        }

        LOG.info(
            String.format("database %s, table %s processed", key.toString(), value.toString()));
      } catch (HiveMetastoreException e) {
        throw new IOException(
            String.format(
                "database %s, table %s got exception", key.toString(), value.toString()), e);
      }

    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      worker.cleanup();
    }
  }

  public static class Stage1ProcessTableMapperWithTextInput
      extends Mapper<LongWritable, Text, LongWritable, Text> {
    private TableCompareWorker worker = new TableCompareWorker();

    protected void setup(Context context) throws IOException, InterruptedException {
      try {
        worker.setup(context);
      } catch (ConfigurationException e) {
        throw new IOException("Invalid configuration", e);
      }
    }

    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException {
      try {
        String [] columns = value.toString().split("\\.");
        if (columns.length != 2) {
          LOG.error(String.format("invalid input at line %d: %s", key.get(), value.toString()));
          return;
        }

        for (String result : worker.processTable(columns[0], columns[1])) {
          context.write(new LongWritable((long)result.hashCode()), new Text(result));
        }

        LOG.info(
            String.format("database %s, table %s processed", key.toString(), value.toString()));
      } catch (HiveMetastoreException e) {
        throw new IOException(
            String.format(
                "database %s, table %s got exception", key.toString(), value.toString()), e);
      }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      worker.cleanup();
    }
  }

  /**
   * Creates a new temporary directory under the temporary directory root.
   *
   * @param conf Configuration containing the directory for temporary files on HDFS
   * @return A path to a new and unique directory under the temporary directory
   * @throws IOException if there's an error creating the temporary directory
   */
  private static Path createTempDirectory(Configuration conf) throws IOException {
    Path tmpRoot = new Path(conf.get(ConfigurationKeys.DEST_HDFS_TMP));
    String uuid = String.format("reair_%d_%s",
        System.currentTimeMillis(),
        UUID.randomUUID().toString());
    Path tmpDir = new Path(tmpRoot, uuid);
    FileSystem fs = tmpDir.getFileSystem(conf);
    fs.mkdirs(tmpDir);
    LOG.info(String.format("Registering %s to be deleted on exit", tmpDir));
    fs.deleteOnExit(tmpDir);
    return tmpDir;
  }

  /**
   * Copies a files.
   * @param srcFile File to copy from.
   * @param destFile File to copy to. The file should not exist.
   * @throws IOException if there is an error copying the file.
   */
  private static void copyFile(Path srcFile, Path destFile) throws IOException {
    String[] copyArgs = {"-cp", srcFile.toString(), destFile.toString()};

    FsShell shell = new FsShell();
    try {
      LOG.debug("Using shell to copy with args " + Arrays.asList(copyArgs));
      ToolRunner.run(shell, copyArgs);
    } catch (Exception e) {
      throw new IOException(e);
    } finally {
      shell.close();
    }
  }
}
