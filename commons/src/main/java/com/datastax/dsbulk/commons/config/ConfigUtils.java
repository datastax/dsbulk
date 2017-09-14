/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.config;

import com.typesafe.config.ConfigException;

public class ConfigUtils {
  public static void badConfigToIllegalArgument(ConfigException e, String path) {
    //This will happen if a user provides the wrong type
    if (e instanceof ConfigException.WrongType) {
      String em = e.getMessage();
      int starting_index = em.indexOf(":", em.indexOf(":") + 1) + 2;
      String errorMsg = em.substring(starting_index);
      throw new IllegalArgumentException(
          "Configuration entry of "
              + path
              + "."
              + errorMsg
              + ". See reference.conf or help for more info");
    }
    // This will catch any parse or missing exception edge cases.
    else {
      throw new IllegalArgumentException(e.getMessage());
    }
  }
}
