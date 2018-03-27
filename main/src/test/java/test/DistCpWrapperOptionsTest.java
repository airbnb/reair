package test;

import static org.junit.Assert.assertEquals;

import com.airbnb.reair.common.DistCpWrapperOptions;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class DistCpWrapperOptionsTest {
  @Test
  public void testGetDistCpTimeout() {
    DistCpWrapperOptions distCpWrapperOptions = new DistCpWrapperOptions(null, null, null, null);
    distCpWrapperOptions.setDistCpJobTimeout(1_000L);
    assertEquals(1_000L, distCpWrapperOptions.getDistcpTimeout(Arrays.asList(), 100L));

    distCpWrapperOptions.setDistcpDynamicJobTimeoutEnabled(false);
    assertEquals(1_000L, distCpWrapperOptions.getDistcpTimeout(Arrays.asList(), 100L));
  }

  @Test
  public void testComputeLongestMapper() {
    DistCpWrapperOptions distCpWrapperOptions = new DistCpWrapperOptions(null, null, null, null);

    List<Long> testCase1 = Arrays.asList(0L, 0L, 0L);
    long procs1 = 500L;
    assertEquals(0L, distCpWrapperOptions.computeLongestMapper(testCase1, procs1));

    List<Long> testCase2 = Arrays.asList();
    long procs2 = 2L;
    assertEquals(0L, distCpWrapperOptions.computeLongestMapper(testCase2, procs2));

    List<Long> testCase3 = Arrays.asList();
    long procs3 = 0L;
    assertEquals(0L, distCpWrapperOptions.computeLongestMapper(testCase3, procs3));

    List<Long> testCase4 = Arrays.asList(100L);
    long procs4 = 1L;
    assertEquals(100L, distCpWrapperOptions.computeLongestMapper(testCase4, procs4));

    List<Long> testCase5 = Arrays.asList(100L);
    long procs5 = 2L;
    assertEquals(100L, distCpWrapperOptions.computeLongestMapper(testCase5, procs5));

    List<Long> testCase6 = Arrays.asList(100L, 1L);
    long procs6 = 2L;
    assertEquals(100L, distCpWrapperOptions.computeLongestMapper(testCase6, procs6));

    List<Long> testCase7 = Arrays.asList(100L, 100L, 100L);
    long procs7 = 1L;
    assertEquals(300L, distCpWrapperOptions.computeLongestMapper(testCase7, procs7));

    List<Long> testCase8 = Arrays.asList(100L, 50L, 51L);
    long procs8 = 2L;
    assertEquals(101L, distCpWrapperOptions.computeLongestMapper(testCase8, procs8));

    List<Long> testCase9 = Arrays.asList(100L, 50L, 50L, 50L, 51L);
    long procs9 = 3L;
    assertEquals(101L, distCpWrapperOptions.computeLongestMapper(testCase9, procs9));

    List<Long> testCase10 = Arrays.asList(100L, 50L, 50L, 50L, 50L);
    long procs10 = 2L;
    assertEquals(150L, distCpWrapperOptions.computeLongestMapper(testCase10, procs10));
  }

  @Test
  public void testGetDistCpTimeout_Dynamic() {
    DistCpWrapperOptions distCpWrapperOptions = new DistCpWrapperOptions(null, null, null, null);
    distCpWrapperOptions.setDistcpDynamicJobTimeoutEnabled(true);
    distCpWrapperOptions.setDistcpDynamicJobTimeoutBase(100L);
    distCpWrapperOptions.setDistcpDynamicJobTimeoutMax(9999L);
    distCpWrapperOptions.setDistcpDynamicJobTimeoutMsPerGbPerMapper(10L);
    // test empty files
    assertEquals(100L, distCpWrapperOptions.getDistcpTimeout(Arrays.asList(0L, 0L), 2L));

    // test no files
    assertEquals(100L, distCpWrapperOptions.getDistcpTimeout(Arrays.asList(), 2L));

    // test 1 file
    assertEquals(110L, distCpWrapperOptions.getDistcpTimeout(Arrays.asList(1_000_000_000L), 2L));

    // normal test case
    assertEquals(120L, distCpWrapperOptions.getDistcpTimeout(
        Arrays.asList(1_000_000_000L, 500_000_000L, 500_000_001L), 2L));

    // test greater than max
    assertEquals(9999L, distCpWrapperOptions.getDistcpTimeout(Arrays.asList(Long.MAX_VALUE), 1L));
  }
}
