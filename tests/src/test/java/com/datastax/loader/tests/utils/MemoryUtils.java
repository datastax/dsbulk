/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.tests.utils;

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
