package com.airbnb.reair.common;

import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;

/**
 * Keys used in the parameters map of a Hive Thrift object for storing Airbnb metadata.
 */
public class HiveParameterKeys {
  // Metadata indicating where this table was copied from
  public static final String SRC_CLUSTER = "abb_source_cluster";

  // Official Hive param keys
  // Last modification time
  public static final String TLDT = hive_metastoreConstants.DDL_TIME;

  // Last modification time for some cases
  public static final String TLMT = "last_modified_time";
}
