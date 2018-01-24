/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.utils;

import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;

/** */
public class MemoryUtils {

  /**
   * Returns the system's free memory in megabytes.
   *
   * <p>This includes the free physical memory + the free swap memory.
   */
  public static long getFreeMemoryMB() {
    OperatingSystemMXBean bean =
        (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    return (bean.getFreePhysicalMemorySize() + bean.getFreeSwapSpaceSize()) / 1024 / 1024;
  }
}
