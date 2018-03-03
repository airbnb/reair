package test;

import com.airbnb.reair.common.HiveObjectSpec;
import com.airbnb.reair.db.DbConnectionFactory;
import com.airbnb.reair.db.EmbeddedMySqlDb;
import com.airbnb.reair.db.StaticDbConnectionFactory;
import com.airbnb.reair.incremental.ReplicationOperation;
import com.airbnb.reair.incremental.ReplicationStatus;
import com.airbnb.reair.incremental.db.PersistedJobInfo;
import com.airbnb.reair.incremental.db.PersistedJobInfoCreator;
import com.airbnb.reair.incremental.db.PersistedJobInfoStore;
import com.airbnb.reair.utils.ReplicationTestUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class PersistedJobInfoCreatorTest {
  private static EmbeddedMySqlDb embeddedMySqlDb;
  private static DbConnectionFactory dbConnectionFactory;
  private static final String MYSQL_TEST_DB_NAME = "replication_test";
  private static final String MYSQL_TEST_TABLE_NAME = "replication_jobs";

  @BeforeClass
  public static void setupClass() throws ClassNotFoundException, SQLException {
    embeddedMySqlDb = new EmbeddedMySqlDb();
    embeddedMySqlDb.startDb();

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
    Connection connection1 = dbConnectionFactory.getConnection();
    Statement statement1 = connection1.createStatement();
    statement1.execute(PersistedJobInfoStore.getCreateTableSql(MYSQL_TEST_TABLE_NAME));
    connection.close();
    connection1.close();
  }

  @Test
  public void testCreateOne() throws Exception {
    PersistedJobInfoCreator jobInfoCreator = new PersistedJobInfoCreator(dbConnectionFactory, MYSQL_TEST_TABLE_NAME);

    Long time = System.currentTimeMillis() / 1000 * 1000;
    HiveObjectSpec hiveObjectSpec = new HiveObjectSpec(
        "a","b");
    List<String> srcPartitionNames = new ArrayList<>();
    CompletableFuture<PersistedJobInfo> persistedJobInfoCompletableFuture =
        jobInfoCreator.createLater(
          time,
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
    assertFalse(persistedJobInfoCompletableFuture.isDone());
    jobInfoCreator.completeFutures();
    assertTrue(persistedJobInfoCompletableFuture.isDone());
  }

  @Test
  public void testCreateManyWithMany() throws Exception {
    PersistedJobInfoCreator jobInfoCreator = new PersistedJobInfoCreator(dbConnectionFactory, MYSQL_TEST_TABLE_NAME);

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

    List<List<CompletableFuture<PersistedJobInfo>>> actualResults = new ArrayList<>();

    for (List<String> ll : expectedResults) {
      List<CompletableFuture<PersistedJobInfo>> subResults = new ArrayList<>();
      for (String srcCluster : ll) {
        CompletableFuture<PersistedJobInfo> persistedJobInfoCompletableFuture =
            jobInfoCreator.createLater(
                System.currentTimeMillis() / 1000 * 1000,
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
        subResults.add(persistedJobInfoCompletableFuture);
      }
      actualResults.add(subResults);
    }
    jobInfoCreator.completeFutures();
    for (int i = 0; i < expectedResults.size(); i++) {
      assertEquals(expectedResults.get(i).size(), actualResults.get(i).size());
      for (int j = 0; j < expectedResults.get(i).size(); j++) {
        assertEquals(expectedResults.get(i).get(j), actualResults.get(i).get(j).get().getSrcClusterName());
      }
    }
    jobInfoCreator.completeFutures();
  }

  @Test
  public void testCreateNone() throws SQLException {
    PersistedJobInfoCreator jobInfoCreator = new PersistedJobInfoCreator(dbConnectionFactory, MYSQL_TEST_TABLE_NAME);
    jobInfoCreator.completeFutures();
  }
}
