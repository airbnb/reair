package com.airbnb.reair.hive.hooks;

import com.airbnb.reair.db.DbCredentials;
import com.airbnb.reair.utils.RetryableTask;
import com.airbnb.reair.utils.RetryingTaskRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.hooks.ExecuteWithHookContext;
import org.apache.hadoop.hive.ql.hooks.HookContext;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Set;

/**
 * A post-execute hook that writes information about successfully executed
 * queries into a MySQL DB. Every successful query generates an entry in an
 * audit log table. In addition, every output object for a successful query
 * generates entries in the output objects table, and the map reduce stats
 * tables.
 */
public class CliAuditLogHook implements ExecuteWithHookContext {

  public static Logger LOG = Logger.getLogger(CliAuditLogHook.class);

  // Number of attempts to make
  private static final int NUM_ATTEMPTS = 10;
  // Will wait BASE_SLEEP * 2 ^ (attempt no.) between attempts
  private static final int BASE_SLEEP = 1;

  public static String DB_USERNAME =
      "airbnb.reair.audit_log.db.username";
  public static String DB_PASSWORD =
      "airbnb.reair.audit_log.db.password";
  // Keys for values in hive-site.xml
  public static String JDBC_URL_KEY = "airbnb.reair.audit_log.jdbc_url";

  protected DbCredentials dbCreds;

  public CliAuditLogHook() {
  }

  // Constructor used for testing
  public CliAuditLogHook(DbCredentials dbCreds) {
    this.dbCreds = dbCreds;
  }

  protected DbCredentials getDbCreds(Configuration conf) {
    if (dbCreds == null) {
      dbCreds = new ConfigurationDbCredentials(conf, DB_USERNAME, DB_PASSWORD);
    }
    return dbCreds;
  }

  @Override
  public void run(HookContext hookContext) throws Exception {
    Set<ReadEntity> inputs = hookContext.getInputs();
    Set<WriteEntity> outputs = hookContext.getOutputs();
    UserGroupInformation ugi = hookContext.getUgi();

    run(hookContext, inputs, outputs, ugi);
  }

  /**
   *
   * @param hookContext
   *     The hook context passed to each hooks.
   * @throws Exception if there's an error
   */
  public void run(final HookContext hookContext,
                  final Set<ReadEntity> readEntities,
                  final Set<WriteEntity> writeEntities,
                  final UserGroupInformation userGroupInformation)
      throws Exception {
    HiveConf conf = hookContext.getConf();
    SessionStateLite sessionStateLite = new SessionStateLite(hookContext.getQueryPlan());

    final DbCredentials dbCreds = getDbCreds(conf);

    final String jdbcUrl = conf.get(JDBC_URL_KEY);
    if (jdbcUrl == null) {
      throw new ConfigurationException(JDBC_URL_KEY
          + " is not defined in the conf!");
    }

    RetryingTaskRunner runner = new RetryingTaskRunner(NUM_ATTEMPTS,
        BASE_SLEEP);

    long startTime = System.currentTimeMillis();
    LOG.debug("Starting insert into audit log");
    runner.runWithRetries(new RetryableTask() {
      @Override
      public void run() throws Exception {
        Connection connection = DriverManager.getConnection(jdbcUrl,
            dbCreds.getReadWriteUsername(),
            dbCreds.getReadWritePassword());
        connection.setTransactionIsolation(
            Connection.TRANSACTION_READ_COMMITTED);

        // Turn off auto commit so that we can ensure that both the
        // audit log entry and the output rows appear at the same time.
        connection.setAutoCommit(false);

        runLogModules(
            connection,
            sessionStateLite,
            readEntities,
            writeEntities,
            userGroupInformation);

        connection.commit();
      }
    });
    LOG.debug(String.format("Applying log modules took %d ms",
        System.currentTimeMillis() - startTime));
  }

  /**
   * Runs the individual audit log modules that make up this hook.
   *
   * @param connection connection to the DB for inserting data
   * @param sessionStateLite the session state that contains relevant config
   * @param readEntities the entities that were read by the query
   * @param writeEntities the entities that were written by the query
   * @param userGroupInformation information about the user that ran the query
   * @return the id column in sql for the core audit log entry for the query
   *
   * @throws Exception if there's an error running the modules
   */
  protected long runLogModules(final Connection connection,
                               final SessionStateLite sessionStateLite,
                               final Set<ReadEntity> readEntities,
                               final Set<WriteEntity> writeEntities,
                               final UserGroupInformation userGroupInformation)
      throws Exception {
    long auditLogId = new AuditCoreLogModule(
                              connection,
                              sessionStateLite,
                              readEntities,
                              writeEntities,
                              userGroupInformation).run();
    new ObjectLogModule(
            connection,
            sessionStateLite,
            readEntities,
            writeEntities,
            auditLogId).run();
    new MapRedStatsLogModule(
            connection,
            sessionStateLite,
            auditLogId).run();

    return auditLogId;
  }
}
