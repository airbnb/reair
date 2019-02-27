package com.airbnb.reair.hive.hooks;

import com.airbnb.reair.db.DbCredentials;
import com.airbnb.reair.utils.RetryingTaskRunner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.MetaStoreEventListener;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.events.AddPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterPartitionEvent;
import org.apache.hadoop.hive.metastore.events.AlterTableEvent;
import org.apache.hadoop.hive.metastore.events.CreateDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.CreateTableEvent;
import org.apache.hadoop.hive.metastore.events.DropDatabaseEvent;
import org.apache.hadoop.hive.metastore.events.DropPartitionEvent;
import org.apache.hadoop.hive.metastore.events.DropTableEvent;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.HashSet;
import java.util.Set;

/**
 * Audit logging for the metastore Thrift server. Comprises of a series of
 * event listeners adding auditing to the various Thrift events.
 */
public class MetastoreAuditLogListener extends MetaStoreEventListener {
  public static Logger LOG = Logger.getLogger(MetastoreAuditLogListener.class);

  // Number of attempts to make.
  private static final int NUM_ATTEMPTS = 10;

  // Will wait BASE_SLEEP * 2 ^ (attempt no.) between attempts.
  private static final int BASE_SLEEP = 1;

  public static String DB_USERNAME =
      "airbnb.reair.metastore.audit_log.db.username";

  public static String DB_PASSWORD =
      "airbnb.reair.metastore.audit_log.db.password";

  // Keys for values in hive-site.xml.
  public static String JDBC_URL_KEY =
      "airbnb.reair.metastore.audit_log.jdbc_url";

  protected DbCredentials dbCredentials;
  protected Connection dbConnection;

  /**
   * Constructor which defines the relevant DB credentials.
   *
   * @param config The resource configuration
   */
  public MetastoreAuditLogListener(Configuration config) {
    super(config);

    dbCredentials = new ConfigurationDbCredentials(
        getConf(),
        DB_USERNAME,
        DB_PASSWORD
    );
  }

  /**
   * Listener which fires when a table is created.
   *
   * <p>For auditing purposes the read/write differential is the non-existence
   * and existence of the created table respectively.</p>
   *
   * @param event The create table event
   */
  @Override
  public void onCreateTable(CreateTableEvent event) throws MetaException {
    try {
      Set<ReadEntity> readEntities = new HashSet<>();
      Set<WriteEntity> writeEntities = new HashSet<>();

      writeEntities.add(
          new WriteEntity(
              new Table(event.getTable()),
              WriteType.INSERT
          )
      );

      run(readEntities, writeEntities, HiveOperation.THRIFT_CREATE_TABLE);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Listener which fires when a table is dropped.
   *
   * <p>For auditing purposes the read/write differential is the existence and
   * non-existence of the dropped table respectively.</p>
   *
   * @param event The drop table event
   */
  @Override
  public void onDropTable(DropTableEvent event) throws MetaException {
    try {
      Set<ReadEntity> readEntities = new HashSet<>();
      readEntities.add(new ReadEntity(new Table(event.getTable())));
      Set<WriteEntity> writeEntities = new HashSet<>();

      run(readEntities, writeEntities, HiveOperation.THRIFT_DROP_TABLE);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Listener which fires when a table is altered.
   *
   * <p>For auditing purposes the read/write differential is the old and new
   * table respectively.</p>
   *
   * @param event The add partition event
   */
  @Override
  public void onAlterTable(AlterTableEvent event) throws MetaException {
    try {
      Set<ReadEntity> readEntities = new HashSet<>();
      readEntities.add(new ReadEntity(new Table(event.getOldTable())));
      Set<WriteEntity> writeEntities = new HashSet<>();

      writeEntities.add(
          new WriteEntity(
              new Table(event.getNewTable()),
              WriteType.INSERT
          )
      );

      run(readEntities, writeEntities, HiveOperation.THRIFT_ALTER_TABLE);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Listener which fires when a partition is added.
   *
   * <p>For auditing purposes the read/write differential is the non-existence
   * and existence of the added partition respectively.</p>
   *
   * @param event The add partition event
   */
  @Override
  public void onAddPartition(AddPartitionEvent event) throws MetaException {
    try {
      Table table = new Table(event.getTable());
      Set<ReadEntity> readEntities = new HashSet<>();
      Set<WriteEntity> writeEntities = new HashSet<>();

      for (org.apache.hadoop.hive.metastore.api.Partition partition :
          event.getPartitions()) {
        writeEntities.add(
            new WriteEntity(
                new Partition(table, partition),
                WriteType.INSERT
            )
        );
      }

      run(readEntities, writeEntities, HiveOperation.THRIFT_ADD_PARTITION);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Listener which fires when a partition is dropped.
   *
   * <p>For auditing purposes the read/write differential is the existence and
   * non-existence of the dropped partition respectively.</p>
   *
   * @param event The drop partition event
   */
  @Override
  public void onDropPartition(DropPartitionEvent event) throws MetaException {
    try {
      Set<ReadEntity> readEntities = new HashSet<>();

      readEntities.add(
          new ReadEntity(
            new Partition(new Table(event.getTable()), event.getPartition())
          )
      );

      Set<WriteEntity> writeEntities = new HashSet<>();

      run(readEntities, writeEntities, HiveOperation.THRIFT_DROP_PARTITION);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Listener which fires when a partition is altered.
   *
   * <p>For auditing purposes the read/write differential is the old and new
   * partition respectively.</p>
   *
   * <p>Note that a bug in the AlterPartitionEvent which has been resolved in
   * a later version does not provide access to the underlying table associated
   * with the partitions, hence it is necessary to fetch it from the metastore.
   * </p>
   *
   * @param event The add partition event
   */
  @Override
  public void onAlterPartition(AlterPartitionEvent event) throws MetaException {
    try {

      // Table is invariant and thus an arbitrary choice between old and new.
      Table table = new Table(
          event.getHandler().get_table(
              event.getOldPartition().getDbName(),
              event.getOldPartition().getTableName()
          )
      );

      Set<ReadEntity> readEntities = new HashSet<>();

      readEntities.add(
          new ReadEntity(
            new Partition(table, event.getOldPartition())
          )
      );

      Set<WriteEntity> writeEntities = new HashSet<>();

      writeEntities.add(
          new WriteEntity(
              new Partition(table, event.getNewPartition()),
            WriteType.INSERT
        )
      );

      run(readEntities, writeEntities, HiveOperation.THRIFT_ALTER_PARTITION);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Listener which fires when a database (schema) is created.
   *
   * <p>For auditing purposes the read/write differential is the non-existence
   * and existence of the created database respectively.</p>
   *
   * @param event The create database event
   */
  @Override
  public void onCreateDatabase(CreateDatabaseEvent event) throws MetaException {
    try {
      Set<ReadEntity> readEntities = new HashSet<>();
      Set<WriteEntity> writeEntities = new HashSet<>();
      writeEntities.add(new WriteEntity(event.getDatabase(), WriteType.INSERT));

      run(readEntities, writeEntities, HiveOperation.THRIFT_CREATE_DATABASE);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Listener which fires when a database (schema) is dropped.
   *
   * <p>For auditing purposes the read/write differential is the existence and
   * non-existence of the dropped database respectively.</p>
   *
   * @param event The drop database event
   */
  @Override
  public void onDropDatabase(DropDatabaseEvent event) throws MetaException {
    try {
      Set<ReadEntity> readEntities = new HashSet<>();
      readEntities.add(new ReadEntity(event.getDatabase()));
      Set<WriteEntity> writeEntities = new HashSet<>();

      run(readEntities, writeEntities, HiveOperation.THRIFT_DROP_DATABASE);
    } catch (Exception e) {
      throw new MetaException(e.getMessage());
    }
  }

  /**
   * Runs the individual metastore audit log modules that make up this hook.
   *
   * @param readEntities The entities that were read by the query
   * @param writeEntities The entities that were written by the query
   * @param hiveOperation The Hive operation
   * @throws Exception If there is an error running any of the logging modules
   */
  private void run(
      Set<ReadEntity> readEntities,
      Set<WriteEntity> writeEntities,
      HiveOperation hiveOperation
  ) throws Exception {
    HiveConf conf = (HiveConf) getConf();

    SessionStateLite sessionStateLite = new SessionStateLite(
        "THRIFT_API",
        hiveOperation,
        conf
    );

    RetryingTaskRunner runner = new RetryingTaskRunner(
        NUM_ATTEMPTS,
        BASE_SLEEP
    );

    long startTime = System.currentTimeMillis();
    LOG.debug("Starting insert into metastore audit log");

    runner.runWithRetries(() -> {
      Connection connection = getConnection();

      synchronized (connection) {
        connection.setTransactionIsolation(
            Connection.TRANSACTION_READ_COMMITTED
        );

        // Turn off auto commit so that we can ensure that both the
        // audit log entry and the output rows appear at the same time.
        connection.setAutoCommit(false);

        long auditLogId = new AuditCoreLogModule(
            connection,
            sessionStateLite,
            readEntities,
            writeEntities,
            null
        ).run();

        new ObjectLogModule(
            connection,
            sessionStateLite,
            readEntities,
            writeEntities,
            auditLogId
        ).run();

        connection.commit();
      }
    });

    LOG.debug(
        String.format(
            "Applying log modules took %d ms",
            System.currentTimeMillis() - startTime
        )
    );
  }

  private synchronized Connection getConnection() throws Exception {
    if (dbConnection == null) {
      HiveConf conf = (HiveConf) getConf();
      String jdbcUrl = conf.get(JDBC_URL_KEY);

      if (jdbcUrl == null) {
        throw new ConfigurationException(
            JDBC_URL_KEY + " is not defined in the conf!"
        );
      }

      dbConnection = DriverManager.getConnection(
          jdbcUrl,
          dbCredentials.getReadWriteUsername(),
          dbCredentials.getReadWritePassword()
      );
    }

    return dbConnection;
  }
}
