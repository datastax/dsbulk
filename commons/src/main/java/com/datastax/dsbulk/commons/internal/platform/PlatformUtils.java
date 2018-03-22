/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.internal.platform;

public class PlatformUtils {

  /**
   * Checks if the operating system is a Windows one.
   *
   * @return <code>true</code> if the operating system is a Windows one, <code>false</code>
   *     otherwise.
   */
  public static boolean isWindows() {
    String osName = System.getProperty("os.name");
    return osName != null && osName.startsWith("Windows");
  }

  /**
   * Checks if the current terminal is an ANSI-compatible terminal.
   *
   * @return <code>true</code> if the terminal is ANSI-compatible, <code>false</code> otherwise.
   */
  public static boolean isAnsi() {
    String term = System.getenv("TERM");
    return term != null && term.toLowerCase().contains("xterm");
  }
}
