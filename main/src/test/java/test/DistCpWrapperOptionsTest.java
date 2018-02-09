package test;

import static org.junit.Assert.assertEquals;

import com.airbnb.reair.common.DistCpWrapperOptions;

import org.junit.Test;

public class DistCpWrapperOptionsTest {
  private static final long ONE_BILLION = 1_000_000_000L;
  
  @Test
  public void testGetDistCpTimeout() {
    DistCpWrapperOptions distCpWrapperOptions = new DistCpWrapperOptions(null, null, null, null);
    distCpWrapperOptions.setDistCpJobTimeout(1_000L);
    assertEquals(1_000L, distCpWrapperOptions.getDistcpTimeout(100L));

    distCpWrapperOptions.setDistcpDynamicJobTimeoutEnabled(false);
    assertEquals(1_000L, distCpWrapperOptions.getDistcpTimeout(100L));

    distCpWrapperOptions.setDistcpDynamicJobTimeoutEnabled(true);
    distCpWrapperOptions.setDistcpDynamicJobTimeoutMin(100);
    distCpWrapperOptions.setDistcpDynamicJobTimeoutMsPerGb(2);
    distCpWrapperOptions.setDistcpDynamicJobTimeoutMax(9_999);

    assertEquals(100L, distCpWrapperOptions.getDistcpTimeout(ONE_BILLION * 1L));
    assertEquals(1_000L, distCpWrapperOptions.getDistcpTimeout(ONE_BILLION * 500L));
    assertEquals(9_999L, distCpWrapperOptions.getDistcpTimeout(ONE_BILLION * 10_000L));
  }
}
