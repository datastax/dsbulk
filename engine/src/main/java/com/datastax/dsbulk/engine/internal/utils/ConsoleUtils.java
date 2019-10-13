/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.utils;

import com.datastax.dsbulk.commons.internal.platform.PlatformUtils;

public class ConsoleUtils {

  private static final String COLUMNS_ENV_NAME = "COLUMNS";
  private static final int DEFAULT_LINE_LENGTH = 150;

  public static final int LINE_LENGTH = ConsoleUtils.getLineLength();

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
