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

public class ConsoleUtils {

  /** The console's line length, if could be detected, otherwise this will report 150. */
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
}
