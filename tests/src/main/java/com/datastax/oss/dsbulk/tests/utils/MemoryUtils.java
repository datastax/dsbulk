/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.tests.utils;

import com.sun.management.OperatingSystemMXBean;
import java.lang.management.ManagementFactory;

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
