package test;

import static org.junit.Assert.assertEquals;

import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.db.DbConnectionFactory;
import com.airbnb.reair.db.EmbeddedMySqlDb;
import com.airbnb.reair.db.StaticDbConnectionFactory;
import com.airbnb.reair.incremental.ReplicationOperation;
import com.airbnb.reair.incremental.ReplicationStatus;
import com.airbnb.reair.incremental.StateUpdateException;
import com.airbnb.reair.incremental.db.PersistedJobInfo;
import com.airbnb.reair.incremental.db.PersistedJobInfoCreator;
import com.airbnb.reair.incremental.db.PersistedJobInfoStore;
import com.airbnb.reair.utils.ReplicationTestUtils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class PersistedJobInfoStoreTest {
  private static final Log LOG = LogFactory.getLog(PersistedJobInfoStoreTest.class);

  private static EmbeddedMySqlDb embeddedMySqlDb;
  private static final String MYSQL_TEST_DB_NAME = "replication_test";
  private static final String MYSQL_TEST_TABLE_NAME = "replication_jobs";

  /**
   * Setups up this class for testing.
   *
   * @throws ClassNotFoundException if there's an error initializing the JDBC driver
   * @throws SQLException if there's an error querying the database
   */
  @BeforeClass
  public static void setupClass() throws ClassNotFoundException, SQLException {
    // Create the MySQL process
    embeddedMySqlDb = new EmbeddedMySqlDb();
    embeddedMySqlDb.startDb();

    // Create the DB within MySQL
    Class.forName("com.mysql.jdbc.Driver");
    String username = embeddedMySqlDb.getUsername();
    String password = embeddedMySqlDb.getPassword();
    Connection connection = DriverManager
        .getConnection(ReplicationTestUtils.getJdbcUrl(embeddedMySqlDb), username, password);
    Statement statement = connection.createStatement();
    String sql = "CREATE DATABASE " + MYSQL_TEST_DB_NAME;
    statement.executeUpdate(sql);
    connection.close();
  }

  @Test
  public void testCreateAndUpdate() throws StateUpdateException, SQLException, Exception {
    // Create the state table
    DbConnectionFactory dbConnectionFactory = new StaticDbConnectionFactory(
        ReplicationTestUtils.getJdbcUrl(embeddedMySqlDb, MYSQL_TEST_DB_NAME),
        embeddedMySqlDb.getUsername(), embeddedMySqlDb.getPassword());



    Connection connection = dbConnectionFactory.getConnection();
    Statement statement = connection.createStatement();
    statement.execute(PersistedJobInfoStore.getCreateTableSql("replication_jobs"));
    final PersistedJobInfoStore jobStore =
        new PersistedJobInfoStore(new Configuration(), dbConnectionFactory, MYSQL_TEST_TABLE_NAME);
    PersistedJobInfoCreator jobInfoCreator =
        new PersistedJobInfoCreator(dbConnectionFactory, MYSQL_TEST_TABLE_NAME);


    // Test out creation
    List<String> partitionNames = new ArrayList<>();
    partitionNames.add("ds=1/hr=1");
    Map<String, String> extras = new HashMap<>();
    extras.put("foo", "bar");
    CompletableFuture<PersistedJobInfo> testJobFuture = jobInfoCreator.createLater(
        ReplicationOperation.COPY_UNPARTITIONED_TABLE,
        ReplicationStatus.PENDING,
        Optional.of(new Path("file:///tmp/test_table")),
        "src_cluster",
        new HiveObjectSpec("test_db", "test_table", "ds=1/hr=1"),
        partitionNames,
        Optional.of("1"),
        Optional.of(new HiveObjectSpec("test_db", "renamed_table", "ds=1/hr=1")),
        Optional.of(new Path("file://tmp/a/b/c")),
        extras);
    jobInfoCreator.completeFutures();
    PersistedJobInfo testJob = testJobFuture.get();

    // Test out retrieval
    Map<Long, PersistedJobInfo> idToJob = new HashMap<>();
    List<PersistedJobInfo> persistedJobInfos = jobStore.getRunnableFromDb();
    for (PersistedJobInfo persistedJobInfo : persistedJobInfos) {
      idToJob.put(persistedJobInfo.getId(), persistedJobInfo);
    }

    // Make sure that the job that was created is the same as the job that
    // was retrieved
    assertEquals(testJob, idToJob.get(testJob.getId()));

    // Try modifying the job
    testJob.setStatus(ReplicationStatus.RUNNING);
    jobStore.persist(testJob);

    // Verify that the change is retrieved
    idToJob.clear();
    persistedJobInfos = jobStore.getRunnableFromDb();
    for (PersistedJobInfo persistedJobInfo : persistedJobInfos) {
      idToJob.put(persistedJobInfo.getId(), persistedJobInfo);
    }
    assertEquals(testJob, idToJob.get(testJob.getId()));
  }

  @AfterClass
  public static void tearDownClass() {
    embeddedMySqlDb.stopDb();
  }
}
