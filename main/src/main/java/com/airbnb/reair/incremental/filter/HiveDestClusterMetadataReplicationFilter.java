package com.airbnb.reair.incremental.filter;

import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.common.NamedPartition;
import com.airbnb.reair.incremental.auditlog.AuditLogEntry;
import com.airbnb.reair.incremental.deploy.ConfigurationKeys;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.api.Table;

/**
 * Filters out objects from the audit log that affect tables that do not have the appropriate
 * destination cluster set in their metadata.
 */
public class HiveDestClusterMetadataReplicationFilter implements ReplicationFilter {

  private static final Log LOG = LogFactory.getLog(RegexReplicationFilter.class);

  public static final String HIVE_TABLE_DEST_CLUSTER_PROP_NAME = "reair_replication_cluster";

  private Configuration conf;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
  }

  @Override
  public boolean accept(AuditLogEntry entry) {
    return true;
  }

  @Override
  public boolean accept(Table table) {
    return accept(table, null);
  }

  @Override
  public boolean accept(Table table, NamedPartition partition) {
    String configDestCluster = conf.get(ConfigurationKeys.DEST_CLUSTER_NAME);
    // Get the dest cluster as specified by the table's metadata
    String tableDestCluster = table.getParameters().get(HIVE_TABLE_DEST_CLUSTER_PROP_NAME);
    if (configDestCluster == null) {
      LOG.warn("Missing value for destination cluster key: " + ConfigurationKeys.DEST_CLUSTER_NAME);
      return true;
    }
    return configDestCluster.equals(tableDestCluster);
  }
}
