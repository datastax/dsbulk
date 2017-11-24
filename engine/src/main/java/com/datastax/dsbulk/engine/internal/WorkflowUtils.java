/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal;

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
import com.google.common.base.Throwables;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
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
   * generally starts to be advantageous when the number of resources is &gt;= 4.
   */
  public static final int TPC_THRESHOLD = 4;

  private static final DateTimeFormatter DEFAULT_TIMESTAMP_PATTERN =
      DateTimeFormatter.ofPattern("uuuuMMdd-HHmmss-SSSSSS");

  public static String newExecutionId(WorkflowType workflowType) {
    return workflowType + "_" + DEFAULT_TIMESTAMP_PATTERN.format(now());
  }

  public static String newCustomExecutionId(String template, WorkflowType workflowType) {
    try {
      // Accepted parameters:
      // 1 : the workflow type
      // 2 : the current time
      // 3 : the JVM process PID, if available
      String executionId =
          String.format(
              template, workflowType, now(), Native.isGetpidAvailable() ? Native.processId() : "");
      if (executionId.isEmpty()) {
        throw new BulkConfigurationException(
            "Could not generate execution ID with template: '"
                + template
                + "': the generated ID is empty.",
            "engine.executionId");
      }
      return executionId;
    } catch (Exception e) {
      throw new BulkConfigurationException(
          "Could not generate execution ID with template: '" + template + "': " + e.getMessage(),
          e,
          "engine.executionId");
    }
  }

  private static ZonedDateTime now() {
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

  public static void checkProductCompatibility(Cluster cluster) {
    Set<Host> hosts = cluster.getMetadata().getAllHosts();
    List<Host> nonDseHosts =
        hosts.stream().filter(host -> host.getDseVersion() == null).collect(Collectors.toList());
    if (nonDseHosts.size() != 0) {
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
