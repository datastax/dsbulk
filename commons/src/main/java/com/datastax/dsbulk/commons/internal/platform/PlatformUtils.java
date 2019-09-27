/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.platform;

import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MICROS;

import com.datastax.oss.driver.internal.core.os.Native;
import com.datastax.oss.driver.shaded.guava.common.base.Splitter;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Random;

public class PlatformUtils {

  /**
   * Checks if the operating system is a Windows one.
   *
   * @return <code>true</code> if the operating system is a Windows one, <code>false</code>
   *     otherwise.
   */
  public static boolean isWindows() {
    String osName = System.getProperty("os.name");
    return osName != null && osName.startsWith("Windows");
  }

  /**
   * Returns this process ID, if available.
   *
   * <p>This implementation first tries to obtain the process ID through a {@linkplain
   * Native#getProcessId() JNI call}; if JNI calls are not available, then it tries to obtain the
   * process ID from the {@linkplain ManagementFactory#getRuntimeMXBean() runtime JMX bean}.
   *
   * <p>If none of this works, this method returns a random integer.
   *
   * @return This process ID if available, or a random integer otherwise.
   */
  public static int pid() {
    if (Native.isGetProcessIdAvailable()) {
      return Native.getProcessId();
    } else {
      try {
        String pidJmx =
            Iterables.get(
                Splitter.on('@').split(ManagementFactory.getRuntimeMXBean().getName()), 0);
        return Integer.parseInt(pidJmx);
      } catch (Exception ignored) {
        return new Random(System.currentTimeMillis()).nextInt();
      }
    }
  }

  @NonNull
  public static ZonedDateTime now() {
    // Try a native call to gettimeofday first since it has microsecond resolution,
    // and fall back to System.currentTimeMillis() if that fails
    if (Native.isCurrentTimeMicrosAvailable()) {
      return EPOCH.plus(Native.currentTimeMicros(), MICROS).atZone(UTC);
    } else {
      return Instant.now().atZone(UTC);
    }
  }
}
