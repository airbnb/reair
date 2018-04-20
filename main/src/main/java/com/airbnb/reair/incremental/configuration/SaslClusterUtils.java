package com.airbnb.reair.incremental.configuration;

import com.airbnb.reair.common.HiveMetastoreException;
import com.airbnb.reair.incremental.deploy.ConfigurationKeys;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.thrift.DelegationTokenIdentifier;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.hadoop.mapreduce.security.TokenCache;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.token.Token;

import java.io.IOException;
import java.net.URI;
import java.util.Map;

public class SaslClusterUtils {

  public static String REAIR_KEY_TOKEN_SIGNATURE_SRC = "mapreduce.lib.reair.token.sig.src";
  public static String REAIR_KEY_TOKEN_SIGNATURE_DEST = "mapreduce.lib.reair.token.sig.dest";

  /**
   * Init hadoop tokens for src and dest metastore.
   *
   * @param credentials Job Credentials
   * @param jobConfig Job Config
   * @throws TokenInitException If tokens init fails
   */
  public static void tokensInit(Credentials credentials, Configuration jobConfig)
          throws TokenInitException {
    try {
      TokenCache.obtainTokensForNamenodes(
          credentials,
          new Path[] {
              new Path(jobConfig.get(ConfigurationKeys.SRC_HDFS_ROOT)),
              new Path(jobConfig.get(ConfigurationKeys.DEST_HDFS_ROOT)) },
          jobConfig);

      URI srcUri = new URI(jobConfig.get(ConfigurationKeys.SRC_CLUSTER_METASTORE_URL));
      SaslClusterUtils.initMetastoreDelegationToken(
          srcUri,
          SaslClusterUtils.REAIR_KEY_TOKEN_SIGNATURE_SRC,
          jobConfig.get(ConfigurationKeys.SRC_METASTORE_PRINCIPAL),
          credentials
      );

      URI destUri = new URI(jobConfig.get(ConfigurationKeys.DEST_CLUSTER_METASTORE_URL));
      SaslClusterUtils.initMetastoreDelegationToken(
          destUri,
          SaslClusterUtils.REAIR_KEY_TOKEN_SIGNATURE_DEST,
          jobConfig.get(ConfigurationKeys.DEST_METASTORE_PRINCIPAL),
          credentials
      );
    } catch (Exception ex) {
      throw new TokenInitException(ex);
    }
  }

  /**
   * Create client for secured metastore service.
   *
   * @param uri An Uri to the metastore service
   * @param signature Token id
   * @param principal User principal
   * @param conf Job configuration
   * @return Metastore client
   * @throws HiveMetastoreException If connecting to the metastore service fails
   */
  public static IMetaStoreClient getMetastoreClient(
          URI uri, String signature, String principal, Configuration conf)
          throws HiveMetastoreException {
    HiveConf hiveConf = getHiveConf(conf, uri, principal);
    String delegationToken = System.getenv(UserGroupInformation.HADOOP_TOKEN_FILE_LOCATION);
    if (delegationToken == null) {
      try {
        delegationToken = createTokenFile(conf);
      } catch (IOException ex) {
        throw new HiveMetastoreException(ex);
      }
    }
    hiveConf.set(MRJobConfig.MAPREDUCE_JOB_CREDENTIALS_BINARY, delegationToken);
    hiveConf.set("hive.metastore.token.signature", signature);

    return createMetastoreClient(hiveConf);
  }

  private static String createTokenFile(Configuration conf) throws IOException {
    FileSystem fs = FileSystem.get(conf);
    Credentials creds = new Credentials();
    fs.addDelegationTokens(UserGroupInformation.getCurrentUser().getUserName(), creds);
    String delegationToken = ".reair-" + System.currentTimeMillis() + ".dt";
    Path filename = new Path(delegationToken);
    creds.writeTokenStorageFile(filename, conf);
    fs.deleteOnExit(filename);
    return delegationToken;
  }

  private static void initMetastoreDelegationToken(
          URI metastoreUri, String signature, String principal, Credentials credentials)
          throws HiveMetastoreException, IOException, TokenInitException {
    Token<DelegationTokenIdentifier> token =
            createMetastoreDelegationToken(metastoreUri, signature, principal);
    credentials.addToken(token.getService(), token);
    UserGroupInformation.getCurrentUser().addToken(token);
  }

  private static Token<DelegationTokenIdentifier> createMetastoreDelegationToken(
          URI uri, String signature, String principal)
          throws HiveMetastoreException, TokenInitException {
    Configuration conf = getConfiguration();
    HiveConf hiveConf = getHiveConf(conf, uri, principal);
    IMetaStoreClient metaStoreClient = createMetastoreClient(hiveConf);

    try {
      String delegationToken = metaStoreClient.getDelegationToken(
              UserGroupInformation.getCurrentUser().getUserName(), principal);
      Token<DelegationTokenIdentifier> token = new Token<DelegationTokenIdentifier>();
      token.decodeFromUrlString(delegationToken);
      token.setService(new Text(signature));
      return token;
    } catch (Exception ex) {
      throw new TokenInitException(ex);
    }
  }

  private static IMetaStoreClient createMetastoreClient(HiveConf hiveConf)
          throws HiveMetastoreException {
    try {
      return new HiveMetaStoreClient(hiveConf);
    } catch (Exception e) {
      throw new HiveMetastoreException(e);
    }
  }

  private static Configuration getConfiguration() {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_AUTHENTICATION, "kerberos");
    return conf;
  }

  private static HiveConf getHiveConf(Configuration conf, URI uri, String principal) {
    HiveConf hiveConf = new HiveConf();
    for (Map.Entry<String, String> entry : conf) {
      hiveConf.set(entry.getKey(), entry.getValue());
    }
    hiveConf.set(HiveConf.ConfVars.METASTORE_USE_THRIFT_SASL.varname, "true");
    hiveConf.set(HiveConf.ConfVars.METASTOREURIS.varname, uri.toString());
    hiveConf.set(HiveConf.ConfVars.METASTORE_KERBEROS_PRINCIPAL.varname, principal);
    return hiveConf;
  }
}
