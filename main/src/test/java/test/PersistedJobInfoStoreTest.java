package test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.db.DbConnectionFactory;
import com.airbnb.reair.db.EmbeddedMySqlDb;
import com.airbnb.reair.db.StaticDbConnectionFactory;
import com.airbnb.reair.incremental.ReplicationOperation;
import com.airbnb.reair.incremental.ReplicationStatus;
import com.airbnb.reair.incremental.StateUpdateException;
import com.airbnb.reair.incremental.db.PersistedJobInfo;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class PersistedJobInfoStoreTest {
  private static final Log LOG = LogFactory.getLog(PersistedJobInfoStoreTest.class);

  private static EmbeddedMySqlDb embeddedMySqlDb;
  private static final String MYSQL_TEST_DB_NAME = "replication_test";
  private static final String MYSQL_TEST_TABLE_NAME = "replication_jobs";

  private static DbConnectionFactory dbConnectionFactory;
  private static PersistedJobInfoStore jobStore;

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

    dbConnectionFactory = new StaticDbConnectionFactory(
        ReplicationTestUtils.getJdbcUrl(embeddedMySqlDb, MYSQL_TEST_DB_NAME),
        embeddedMySqlDb.getUsername(), embeddedMySqlDb.getPassword());
    jobStore =
        new PersistedJobInfoStore(new Configuration(), dbConnectionFactory, MYSQL_TEST_TABLE_NAME);
    Statement statement2 = dbConnectionFactory.getConnection().createStatement();
    statement2.execute(PersistedJobInfoStore.getCreateTableSql("replication_jobs"));
  }

  @Test
  public void testCreateAndUpdate() throws StateUpdateException, SQLException, Exception {
    Connection connection = dbConnectionFactory.getConnection();


    // Test out creation
    List<String> partitionNames = new ArrayList<>();
    partitionNames.add("ds=1/hr=1");
    Map<String, String> extras = new HashMap<>();
    extras.put("foo", "bar");
    PersistedJobInfo testJob = PersistedJobInfo.createDeferred(
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
    jobStore.createMany(Arrays.asList(testJob));

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

  @Test
  public void testCreateOne() throws Exception {
    HiveObjectSpec hiveObjectSpec = new HiveObjectSpec(
        "a","b");
    List<String> srcPartitionNames = new ArrayList<>();
    PersistedJobInfo persistedJobInfoCompletableFuture =
        PersistedJobInfo.createDeferred(
            ReplicationOperation.COPY_PARTITION,
            ReplicationStatus.PENDING,
            Optional.empty(),
            "a",
            hiveObjectSpec,
            srcPartitionNames,
            Optional.empty(),
            Optional.empty(),
            Optional.empty(),
            new HashMap<>());
    jobStore.createMany(Arrays.asList(persistedJobInfoCompletableFuture));
  }

  @Test
  public void testCreateManyWithMany() throws Exception {

    List<List<String>> expectedResults = new ArrayList<>();
    List<String> results1 = new ArrayList<>(Arrays.asList("aa", "ab", "ac", "ad"));
    List<String> results2 = new ArrayList<>(Arrays.asList("ba", "bb", "bc"));
    List<String> results3 = new ArrayList<>(Arrays.asList("cc"));
    List<String> results4 = new ArrayList<>();
    List<String> results5 = new ArrayList<>(Arrays.asList("ea", "eb"));
    expectedResults.add(results1);
    expectedResults.add(results2);
    expectedResults.add(results3);
    expectedResults.add(results4);
    expectedResults.add(results5);

    HiveObjectSpec hiveObjectSpec = new HiveObjectSpec("a", "b");

    List<List<PersistedJobInfo>> actualResults = new ArrayList<>();
    List<PersistedJobInfo> jobs = new ArrayList<>();

    for (List<String> ll : expectedResults) {
      List<PersistedJobInfo> subResults = new ArrayList<>();
      for (String srcCluster : ll) {
        PersistedJobInfo persistedJobInfo =
            PersistedJobInfo.createDeferred(
                ReplicationOperation.COPY_PARTITION,
                ReplicationStatus.PENDING,
                Optional.empty(),
                srcCluster,
                hiveObjectSpec,
                new ArrayList<>(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                new HashMap<>());
        subResults.add(persistedJobInfo);
        jobs.add(persistedJobInfo);
      }
      actualResults.add(subResults);
    }
    jobStore.createMany(jobs);
    for (int i = 0; i < expectedResults.size(); i++) {
      assertEquals(expectedResults.get(i).size(), actualResults.get(i).size());
      for (int j = 0; j < expectedResults.get(i).size(); j++) {
        assertEquals(
            expectedResults.get(i).get(j),
            actualResults.get(i).get(j).getSrcClusterName());
      }
    }
    boolean exceptionThrown = false;
    try {
      jobStore.createMany(jobs);
    } catch (StateUpdateException e) {
      exceptionThrown = true;
    }
    assertTrue(exceptionThrown);
  }

  @Test
  public void testCreateNone() throws StateUpdateException {
    jobStore.createMany(new ArrayList<>());
  }

  @AfterClass
  public static void tearDownClass() {
    embeddedMySqlDb.stopDb();
  }
}
