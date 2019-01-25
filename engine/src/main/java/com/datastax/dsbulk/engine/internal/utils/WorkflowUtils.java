/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.utils;

import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.time.temporal.ChronoUnit.MICROS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Native;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.engine.WorkflowType;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposable;

public class WorkflowUtils {

  private static final Logger LOGGER = LoggerFactory.getLogger(WorkflowUtils.class);

  /**
   * The threshold, in number of resources to read or write, that triggers a thread-per-core
   * optimization.
   *
   * <p>This threshold actually varies a bit depending on the dataset to load or unload, but it
   * generally starts to be advantageous when the number of resources is &gt;= 1/4 of the number of
   * available cores.
   */
  public static final int TPC_THRESHOLD =
      Math.max(4, Runtime.getRuntime().availableProcessors() / 4);

  private static final DateTimeFormatter DEFAULT_TIMESTAMP_PATTERN =
      DateTimeFormatter.ofPattern("uuuuMMdd-HHmmss-SSSSSS");

  public static String newExecutionId(WorkflowType workflowType) {
    return workflowType + "_" + DEFAULT_TIMESTAMP_PATTERN.format(now());
  }

  public static String newCustomExecutionId(
      @NotNull String template, @NotNull WorkflowType workflowType) {
    try {
      // Accepted parameters:
      // 1 : the workflow type
      // 2 : the current time
      // 3 : the JVM process PID, if available
      String executionId = String.format(template, workflowType, now(), pid());
      if (executionId.isEmpty()) {
        throw new BulkConfigurationException(
            "Could not generate execution ID with template: '"
                + template
                + "': the generated ID is empty.");
      }
      return executionId;
    } catch (Exception e) {
      throw new BulkConfigurationException(
          "Could not generate execution ID with template: '" + template + "': " + e.getMessage(),
          e);
    }
  }

  public static int pid() {
    if (Native.isGetpidAvailable()) {
      return Native.processId();
    } else {
      try {
        String pidJmx =
            Iterables.get(
                Splitter.on('@').split(ManagementFactory.getRuntimeMXBean().getName()), 0);
        return Integer.parseInt(pidJmx);
      } catch (Exception ignored) {
        return new java.util.Random(System.currentTimeMillis()).nextInt();
      }
    }
  }

  @NotNull
  public static ZonedDateTime now() {
    // Try a native call to gettimeofday first since it has microsecond resolution,
    // and fall back to System.currentTimeMillis() if that fails
    if (Native.isGettimeofdayAvailable()) {
      return EPOCH.plus(Native.currentTimeMicros(), MICROS).atZone(UTC);
    } else {
      return Instant.now().atZone(UTC);
    }
  }

  public static String formatElapsed(long seconds) {
    long hr = SECONDS.toHours(seconds);
    long min = SECONDS.toMinutes(seconds - HOURS.toSeconds(hr));
    long sec = seconds - HOURS.toSeconds(hr) - MINUTES.toSeconds(min);
    if (hr > 0) {
      return String.format("%d hours, %d minutes and %d seconds", hr, min, sec);
    } else if (min > 0) {
      return String.format("%d minutes and %d seconds", min, sec);
    } else {
      return String.format("%d seconds", sec);
    }
  }

  public static Exception closeQuietly(AutoCloseable closeable, Exception suppressed) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        if (suppressed != null) {
          e.addSuppressed(suppressed);
        }
        return e;
      }
    }
    return suppressed;
  }

  public static Exception closeQuietly(Disposable disposable, Exception suppressed) {
    if (disposable != null && !disposable.isDisposed()) {
      try {
        disposable.dispose();
      } catch (Exception e) {
        // Reactor framework often wraps InterruptedException
        Throwable root = Throwables.getRootCause(e);
        if (root instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        } else {
          if (suppressed != null) {
            e.addSuppressed(suppressed);
          }
          return e;
        }
      }
    }
    return suppressed;
  }

  public static void assertAccessibleFile(Path filePath, String descriptor) {
    if (!Files.exists(filePath)) {
      throw new BulkConfigurationException(
          String.format("%s %s does not exist", descriptor, filePath));
    }
    if (!Files.isRegularFile(filePath)) {
      throw new BulkConfigurationException(
          String.format("%s %s is not a file", descriptor, filePath));
    }
    if (!Files.isReadable(filePath)) {
      throw new BulkConfigurationException(
          String.format("%s %s is not readable", descriptor, filePath));
    }
  }

  public static void checkProductCompatibility(Cluster cluster) {
    Set<Host> hosts = cluster.getMetadata().getAllHosts();
    List<Host> nonDseHosts =
        hosts
            .stream()
            .filter(
                host ->
                    host.getDseVersion() == null && host.getCassandraVersion().getDSEPatch() <= 0)
            .collect(Collectors.toList());
    if (!nonDseHosts.isEmpty()) {
      LOGGER.error(
          "Incompatible cluster detected. Load functionality is only compatible with a DSE cluster.");
      LOGGER.error("The following nodes do not appear to be running DSE:");
      for (Host host : nonDseHosts) {
        LOGGER.error(host.toString());
      }
      throw new IllegalStateException("Unable to load data to non DSE cluster");
    }
  }
}
