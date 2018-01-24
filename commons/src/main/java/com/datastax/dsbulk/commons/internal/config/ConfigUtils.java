/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.config;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.typesafe.config.ConfigException;
import java.util.regex.Matcher;

public class ConfigUtils {

  public static BulkConfigurationException configExceptionToBulkConfigurationException(
      ConfigException e, String path) {
    // This will happen if a user provides the wrong type
    // Error generated will look like this
    // Configuration entry of connector.csv.recursive has type STRING rather than BOOLEAN. See settings.md or help for
    // more info.
    if (e instanceof ConfigException.WrongType) {
      String em = e.getMessage();
      int startingIndex = em.lastIndexOf(":") + 2;
      String errorMsg = em.substring(startingIndex);
      return new BulkConfigurationException(
          "Configuration entry of "
              + path
              + "."
              + errorMsg
              + ". See settings.md or help for more info.",
          e);
    } else if (e instanceof ConfigException.Parse) {
      return new BulkConfigurationException(
          "Configuration entry of "
              + path
              + ". "
              + e.getMessage()
              + ". See settings.md or help for more info.",
          e);
    } else {
      // Catch-all for other types of exceptions.
      return new BulkConfigurationException(e.getMessage(), e);
    }
  }

  public static String maybeEscapeBackslash(String value) {
    return value.replaceAll("\\\\{1,2}", Matcher.quoteReplacement("\\\\"));
  }

  public static boolean containsBackslashError(ConfigException exception) {
    return exception.getMessage().contains("backslash");
  }
}
