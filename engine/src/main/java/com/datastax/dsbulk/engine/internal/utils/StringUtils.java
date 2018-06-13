/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.utils;

import java.util.Collections;

public class StringUtils {

  public static final String DELIMITER = ".";

  /**
   * Upper-cases the first letter of the given string
   *
   * @param s String to convert.
   * @return The string with its first letter in upper case.
   */
  public static String ucfirst(String s) {
    return Character.toUpperCase(s.charAt(0)) + s.substring(1);
  }

  /**
   * Returns a new string consisting of n copies of the given string.
   *
   * @param s String to copy.
   * @param count Number of times to copy it.
   * @return a new string consisting of n copies of the given string.
   */
  public static String nCopies(String s, int count) {
    return String.join("", Collections.nCopies(count, s));
  }

  /**
   * Replaces tokens that are meaningful in html with their entity representations.
   *
   * @param s String to escape.
   * @return the escaped string.
   */
  public static String htmlEscape(String s) {
    return s.replaceAll("<", "&lt;").replaceAll(">", "&gt;");
  }

  /**
   * Left pad a string with spaces to a size of {@code size}.
   *
   * @param s String to pad.
   * @param size the size.
   * @return the padded string.
   */
  public static String leftPad(String s, int size) {
    int repeat = size - s.length();
    if (repeat <= 0) {
      return s;
    }
    char[] buf = new char[repeat];
    for (int i = 0; i < repeat; i++) {
      buf[i] = ' ';
    }
    return new String(buf).concat(s);
  }
}
