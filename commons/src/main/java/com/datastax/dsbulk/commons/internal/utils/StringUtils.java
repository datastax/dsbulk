/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.utils;

import javax.management.ObjectName;
import org.jetbrains.annotations.NotNull;

public class StringUtils {

  /**
   * Returns the given string quoted with {@link javax.management.ObjectName#quote(String)} if it
   * has forbidden characters in a value associated with a key in an JMX object name; otherwise,
   * returns the original string.
   *
   * @param value The value to quote if necessary.
   * @return The value quoted if necessary, or the original value if quoting isn't required.
   */
  public static @NotNull String quoteJMXIfNecessary(@NotNull String value) {
    if (value.contains("\"")
        || value.contains("*")
        || value.contains("?")
        || value.contains("\\")
        || value.contains("\n")) {
      return ObjectName.quote(value);
    }
    return value;
  }
}
