/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons;

/** */
public class PlatformUtils {

  /**
   * Checks if the operating system is a Windows one
   *
   * @return <code>true</code> if the operating system is a Windows one, <code>false</code>
   *     otherwise.
   */
  public static boolean isWindows() {
    String osName = System.getProperty("os.name");
    return osName != null && osName.startsWith("Windows");
  }
}
