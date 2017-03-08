# ReAir

ReAir is a collection of easy-to-use tools for replicating tables and partitions between Hive data warehouses. These tools are targeted at developers that already have some familiarity with operating warehouses based on Hadoop and Hive.

## Overview

The replication features in ReAir are useful for the following use cases:

* Migration of a Hive warehouse
* Incremental replication between two warehouses
* Disaster recovery

When migrating a Hive warehouse, ReAir can be used to copy over existing data to the new warehouse. Because ReAir copies both data and metadata, datasets are ready to query as soon as the copy completes.

While many organizations start out with a single Hive warehouse, they often want better isolation between production and ad hoc workloads. Two isolated Hive warehouses accommodate this need well, and with two warehouses, there is a need to replicate evolving datasets. ReAir can be used to replicate data from one warehouse to another and propagate updates incrementally as they occur.

Lastly, ReAir can be used to replicated datasets to a hot-standby warehouse for fast failover in disaster recovery scenarios.

To accommodate these use cases, ReAir includes both batch and incremental replication tools. Batch replication executes a one-time copy of a list of tables. Incremental replication is a long-running process that copies objects as they are created or changed on the source warehouse.

## Additional Documentation

* [Blog Post](https://medium.com/airbnb-engineering/reair-easy-to-use-tools-for-migrating-and-replicating-petabyte-scale-data-warehouses-5153f8a433da#.lzr9kllqt)
* [FAQ](docs/faq.md)
* [Known Issues](docs/known_issues.md)
* [Large HDFS Directory Copy](docs/hdfs_copy.md)

## Batch Replication

### Prerequisites:

* Hadoop (Most, but tested with 2.5.0)
* Hive (Most, but tested with 0.13)

### Run Batch Replication

* Read through and fill out the configuration from the  [template](main/src/main/resources/batch_replication_configuration_template.xml).
* Switch to the repo directory and build the JAR.

```
cd reair
./gradlew shadowjar -p main -x test
```

* Create a local text file containing the tables that you want to copy. A row in the text file should consist of the DB name and the table name separated by a period. e.g.

```
my_db1.my_table1
my_db2.my_table2
```

* Launch the job using the `hadoop jar` command on the destination, specifying the config file and the list of tables to copy. A larger heap for the client may be needed for large batches, so set `HADOOP_HEAPSIZE` appropriately. Also, depending on how the warehouse is set up, you may need to run the process as a different user (e.g. `hive`).

```
export HADOOP_OPTS="-Dlog4j.configuration=file://<path to log4j.properties>"
export HADOOP_HEAPSIZE=8096
sudo -u hive hadoop jar main/build/libs/airbnb-reair-main-1.0.0-all.jar com.airbnb.reair.batch.hive.MetastoreReplicationJob --config-files my_config_file.xml --table-list my_tables_to_copy.txt
```

* Additional CLI Options: `--step`, `--override-input`. These arguments are useful if want to run one of the three MR job individually for faster failure recovery. `--step` indicates which step to run. `--override-input` provides the path for the input when running the second and third stage MR jobs. The input path will usually be the output for the first stage MR job.

## Incremental Replication

### Prerequisites:

* Hadoop (Most, but tested with 2.5.0)
* Hive (Most, but tested with 0.13)

### Audit Log Hook Setup

Incremental replication relies on recording changes in the source Hive warehouse to figure out what needs to be replicated. These changes can be recorded in two different ways. In the first method, the hook is added to the Hive CLI and runs after a query is successful. In the other method, the hook is added as a listener in the Hive remote metastore server. This method requires that you have the  metastore server deployed and used by Hive, but it will work when systems other than Hive (e.g. Spark) make calls to the metastore server to create tables. The steps to deploy either hook are similar:

Build and deploy the JAR containing the audit log hook

* Switch to the repository directory and build the JAR.

```
cd reair
./gradlew shadowjar -p hive-hooks -x test
```

* Once built, the JAR for the audit log hook can be found under `hive-hooks/build/libs/airbnb-reair-hive-hooks-1.0.0-all.jar`.

* Copy the JAR to the Hive auxiliary library path. The specifics of the path depending on your setup. Generally, the auxiliary library path can be configured using the configuration parameter `hive.aux.jars.path`. If you're deploying the hook for the CLI, you only have to deploy the JAR on the hosts where the CLI will be run, and likewise, if you're deploying the hook for the metastore server, you only have to deploy the JAR on the server host.

* Create and setup the tables on MySQL required for the audit log. You can create the tables by running the create table commands in all of the .sql files [here](hive-hooks/src/main/resources/). If you're planning to use the same DB to store the tables for incremental replication, also run the create table commands [here](main/src/main/resources/create_tables.sql).

* If you want to add the hook for the Hive CLI, change the configuration for the Hive CLI (in the **source** warehouse) to use the audit log hook by adding the following sections to `hive-site.xml` from the [audit log configuration template](hive-hooks/src/main/resources/hook_configuration_template.xml) after replacing with appropriate values.

* If you want to add the hook for the metastore server, change the configuration for the Hive metastore server (in the **source** warehouse) to use the hook by adding the following sections to `hive-site.xml` from the [metastore audit log configuration template](hive-hooks/src/main/resources/listener_configuration_template.xml) after replacing with appropriate values.

* Run a test query and verify that you see the appropriate rows in the `audit_log` and `audit_objects` tables.

### Process Setup

* If the MySQL tables for incremental replication were not set up while setting up the audit log, create the state tables for incremental replication on desired MySQL instance by running the create table commands listed [here](main/src/main/resources/create_tables.sql).

* Read through and fill out the configuration from the [template](main/src/main/resources/replication_configuration_template.xml). You might want to deploy the file to a widely accessible location.

* Switch to the repo directory and build the JAR. You can skip the unit tests if no changes have been made (via the `'-x test'` flag).

```
cd reair
./gradlew shadowjar -p main -x test
```

Once the build finishes, the JAR to run the incremental replication process can be found under `main/build/libs/airbnb-reair-main-1.0.0-all.jar`

* To start replicating, set options to point to the appropriate logging configuration and kick off the replication launcher by using the `hadoop jar` command on the destination cluster. An example `log4j.properties` file is provided [here](main/src/main/resources/log4j.properties). Be sure to specify the configuration file that was filled out in the prior step. As with batch replication, you may need to run the process as a different user.

```
export HADOOP_OPTS="-Dlog4j.configuration=file://<path to log4j.properties>"
sudo -u hive hadoop jar airbnb-reair-main-1.0.0-all.jar com.airbnb.reair.incremental.deploy.ReplicationLauncher --config-files my_config_file.xml
```

If you use the recommended [`log4j.properties`](main/src/main/resources/log4j.properties) file that is shipped with the tool, messages with the `INFO` level will be printed to `stderr`, but more detailed logging messages with >= `DEBUG` logging level will be recorded to a log file in the current working directory.

When the incremental replication process is launched for the first time, it will start replicating entries after the highest numbered ID in the audit log. Because the process periodically checkpoints progress to the DB, it can be killed and will resume from where it left off when restarted. To override this behavior, please see the additional options section.

* Verify that entries are replicated properly by creating a test table on the source warehouse and checking to see if it appears on the destination warehouse.

For production deployment, an external process should monitor and restart the replication process if it exits. The replication process will exit if the number of consecutive failures while making RPCs or DB queries exceed the configured number of retries.

### Additional CLI options:

To force the process to start replicating entries after a particular audit log ID, you can pass the `--start-after-id` parameter:

```
export HADOOP_OPTS="-Dlog4j.configuration=file://<path to log4j.properties>"
hadoop jar main/build/libs/airbnb-reair-main-1.0.0-all.jar com.airbnb.reair.replication.deploy.ReplicationLauncher --config-files my_config_file.xml --start-after-id 123456
```

Replication entries that were started but not completed on the last invocation will be marked as aborted when you use `--start-after-id` to restart the process.

### Monitoring / Web UI:

The incremental replication process starts a Thrift server that can be used to get metrics and view progress. The Thrift definition is provided [here](thrift/src/main/resources/reair.thrift). A simple web server that displays progress has been included in the `web-server` module. To run the web server:

* Switch to the repo directory and build the JAR's. You can skip the unit tests if no changes have been made.

```
cd reair
gradlew shadowjar -p web-server -x test
```

* The JAR containing the web server can be found at

```
web-server/build/libs/airbnb-reair-web-server-1.0.0-all.jar
```

* Start the web server, specifying the appropriate Thrift host and port where the incremental replication process is running.

```
java -jar airbnb-reair-web-server-1.0.0-all.jar --thrift-host localhost --thrift-port 9996 --http-port 8080
```

* Point your browser to the appropriate URL e.g. `http://localhost:8080` to view the active and retired replication jobs.

# Discussion Group
A discussion group is available [here](https://groups.google.com/forum/#!forum/airbnb-reair).

# In the wild
If you find `ReAir` useful, please list yourself on this [page!](docs/inthewild.md)
