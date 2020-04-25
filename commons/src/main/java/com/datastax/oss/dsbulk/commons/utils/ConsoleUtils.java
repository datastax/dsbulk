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
package com.datastax.oss.dsbulk.commons.utils;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

public class ConsoleUtils {

  public static final String BULK_LOADER_APPLICATION_NAME = "DataStax Bulk Loader";

  public static final int LINE_LENGTH = ConsoleUtils.getLineLength();

  private static final String COLUMNS_ENV_NAME = "COLUMNS";

  private static final int DEFAULT_LINE_LENGTH = 150;

  private static int getLineLength() {
    int columns = DEFAULT_LINE_LENGTH;
    String columnsStr = System.getenv(COLUMNS_ENV_NAME);
    if (columnsStr != null) {
      try {
        columns = Integer.parseInt(columnsStr);
      } catch (NumberFormatException ignored) {
      }
      if (PlatformUtils.isWindows()) {
        columns--;
      }
    }
    return columns;
  }

  public static String getBulkLoaderVersion() {
    // Get the version of dsbulk from version.txt.
    String version = "UNKNOWN";
    try (InputStream versionStream = ConsoleUtils.class.getResourceAsStream("/version.txt")) {
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
