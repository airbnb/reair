package com.airbnb.reair.incremental.configuration;

import com.airbnb.reair.incremental.DirectoryCopier;

import org.apache.hadoop.conf.Configuration;

public interface ClusterFactory {

  void setConf(Configuration conf);

  Configuration getConf() throws ConfigurationException;

  Cluster getSrcCluster() throws ConfigurationException;

  Cluster getDestCluster() throws ConfigurationException;

  DirectoryCopier getDirectoryCopier() throws ConfigurationException;
}
