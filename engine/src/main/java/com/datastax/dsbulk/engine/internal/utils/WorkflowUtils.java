/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.utils;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;
import com.datastax.dsbulk.engine.WorkflowType;
import com.datastax.oss.driver.api.core.uuid.Uuids;
import com.datastax.oss.driver.shaded.guava.common.base.Throwables;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.UUID;
import reactor.core.Disposable;

public class WorkflowUtils {

  public static final String BULK_LOADER_APPLICATION_NAME = "DataStax Bulk Loader";

  private static final UUID BULK_LOADER_NAMESPACE =
      UUID.fromString("2505c745-cedf-4714-bcab-0d580270ed95");

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
    return workflowType + "_" + DEFAULT_TIMESTAMP_PATTERN.format(PlatformUtils.now());
  }

  public static String newCustomExecutionId(
      @NonNull String template, @NonNull WorkflowType workflowType) {
    try {
      // Accepted parameters:
      // 1 : the workflow type
      // 2 : the current time
      // 3 : the JVM process PID, if available
      String executionId =
          String.format(template, workflowType, PlatformUtils.now(), PlatformUtils.pid());
      if (executionId.isEmpty()) {
        throw new IllegalStateException("Generated execution ID is empty.");
      }
      return executionId;
    } catch (Exception e) {
      throw new BulkConfigurationException(
          "Could not generate execution ID with template: '" + template + "': " + e.getMessage(),
          e);
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

  public static String getBulkLoaderVersion() {
    // Get the version of dsbulk from version.txt.
    String version = "UNKNOWN";
    try (InputStream versionStream = WorkflowUtils.class.getResourceAsStream("/version.txt")) {
      if (versionStream != null) {
        BufferedReader reader =
            new BufferedReader(new InputStreamReader(versionStream, StandardCharsets.UTF_8));
        version = reader.readLine();
      }
    } catch (Exception e) {
      // swallow
    }
    return version;
  }

  @NonNull
  public static String getBulkLoaderNameAndVersion() {
    String version = getBulkLoaderVersion();
    return BULK_LOADER_APPLICATION_NAME + " v" + version;
  }

  public static UUID clientId(String executionId) {
    return Uuids.nameBased(BULK_LOADER_NAMESPACE, executionId);
  }
}
