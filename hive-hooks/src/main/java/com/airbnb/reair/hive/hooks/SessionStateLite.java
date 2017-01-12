package com.airbnb.reair.hive.hooks;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.MapRedStats;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.session.SessionState;

import java.util.HashMap;
import java.util.Map;

/**
 * Class to provide a lightweight representation of the Hive session state.
 *
 * <p>Regrettably the org.apache.hadoop.hive.ql.plan.HiveOperation enum type
 * only provides a subset of the operations which are available via Thrift and
 * the CLI, hence this class provides a mechanism for including the expanded
 * set.</p>
 */
class SessionStateLite {

  private String cmd;
  private String commandType;
  private String queryId;
  private HiveConf conf;
  private Map<String, MapRedStats> mapRedStats;

  /**
   * Creates a lightweight representation of the session state.
   *
   * @param cmd The Hive command either from Thrift or the CLI
   * @param commandType The Hive command operation
   */
  public SessionStateLite(
      String cmd,
      HiveOperation commandType,
      HiveConf conf
  ) {
    this.cmd = cmd;
    this.commandType = commandType.name();
    this.conf = new HiveConf(conf);
    this.queryId = null;
    this.mapRedStats = null;
  }

  /**
   * Creates a lightweight representation of the session state.
   *
   * @param plan The Hive query plan
   */
  public SessionStateLite(QueryPlan plan) {

    SessionState sessionState = SessionState.get();

    this.conf = new HiveConf(sessionState.getConf());
    this.cmd = plan.getQueryStr();
    this.commandType = plan.getOperationName();
    this.queryId = plan.getQueryId();
    this.mapRedStats = new HashMap<>(sessionState.getMapRedStats());
  }

  /**
   * Gets the Hive command either from Thrift or the CLI.
   *
   * @return The Hive command
   */
  String getCmd() {
    return cmd;
  }

  /**
   * Gets the command type associated with the session state which is defined
   * as the stringified version of the HiveOperation.
   *
   * @return The command type
   */
  String getCommandType() {
    return commandType;
  }

  /**
   * Gets the query ID associated with session state. Note this is undefined
   * for Thrift hooks.
   *
   * @return The query ID
   */
  String getQueryId() {
    return queryId;
  }

  /**
   * Gets the Hive config associated with session state.
   *
   * @return The Hive config
   */
  HiveConf getConf() {
    return conf;
  }

  /**
   * Gets the MapReduce associated with session state. Note this is undefined
   * for Thrift hooks.
   *
   * @return The MapReduce statistics
   */
  Map<String,MapRedStats> getMapRedStats() {
    return mapRedStats;
  }
}
