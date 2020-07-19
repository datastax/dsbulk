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
package com.datastax.oss.dsbulk.workflow.api.utils;

import com.datastax.oss.driver.api.core.uuid.Uuids;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public class WorkflowUtils {

  public static final String BULK_LOADER_APPLICATION_NAME = "DataStax Bulk Loader";

  public static final UUID BULK_LOADER_NAMESPACE =
      UUID.fromString("2505c745-cedf-4714-bcab-0d580270ed95");

  private static final DateTimeFormatter DEFAULT_TIMESTAMP_PATTERN =
      DateTimeFormatter.ofPattern("uuuuMMdd-HHmmss-SSSSSS");

  @NonNull
  public static String newDefaultExecutionId(@NonNull String operationTitle) {
    return operationTitle.toUpperCase()
        + "_"
        + DEFAULT_TIMESTAMP_PATTERN.format(PlatformUtils.now());
  }

  @NonNull
  public static String newCustomExecutionId(
      @NonNull String template, @NonNull String operationTitle) {
    try {
      // Accepted parameters:
      // 1 : the operation title
      // 2 : the current time
      // 3 : the JVM process PID, if available
      String executionId =
          String.format(template, operationTitle, PlatformUtils.now(), PlatformUtils.pid());
      if (executionId.isEmpty()) {
        throw new IllegalStateException("Generated execution ID is empty.");
      }
      return executionId;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Could not generate execution ID with template: '" + template + "': " + e.getMessage(),
          e);
    }
  }

  @NonNull
  public static UUID clientId(String executionId) {
    return Uuids.nameBased(BULK_LOADER_NAMESPACE, executionId);
  }

  @NonNull
  public static String getBulkLoaderVersion() {
    // Get the version of dsbulk from version.txt.
    String version = "UNKNOWN";
    try (InputStream versionStream =
        ConsoleUtils.class.getResourceAsStream("/com/datastax/oss/dsbulk/version.txt")) {
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
}
