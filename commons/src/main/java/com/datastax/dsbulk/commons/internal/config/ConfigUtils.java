/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
