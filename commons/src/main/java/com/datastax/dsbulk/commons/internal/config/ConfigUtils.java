/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.internal.config;

import com.typesafe.config.ConfigException;

public class ConfigUtils {
  public static BulkConfigurationException configExceptionToBulkConfigurationException(
      ConfigException e, String path) {
    // This will happen if a user provides the wrong type
    // Error generated will look like this
    // Configuration entry of connector.csv.recursive has type STRING rather than BOOLEAN. See settings.md or help for
    // more info.
    if (e instanceof ConfigException.WrongType) {
      String em = e.getMessage();
      int starting_index = em.lastIndexOf(":") + 2;
      String errorMsg = em.substring(starting_index);
      return new BulkConfigurationException(
          "Configuration entry of "
              + path
              + "."
              + errorMsg
              + ". See settings.md or help for more info.",
          path);
    } else if (e instanceof ConfigException.Parse) {
      return new BulkConfigurationException(
          "Configuration entry of "
              + path
              + ". "
              + e.getMessage()
              + ". See settings.md or help for more info.",
          path);
    } else {
      // Catch-all for other types of exceptions.
      return new BulkConfigurationException(e.getMessage(), path);
    }
  }
}
