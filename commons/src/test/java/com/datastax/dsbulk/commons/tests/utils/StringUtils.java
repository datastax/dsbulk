/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.tests.utils;

import com.google.common.base.CharMatcher;
import java.net.URL;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class StringUtils {

  private static final ConcurrentMap<String, AtomicInteger> SEQS = new ConcurrentHashMap<>();

  /**
   * Generates a unique CQL identifier with the given prefix.
   *
   * @return a unique CQL identifier.
   * @param prefix the prefix to use.
   */
  public static String uniqueIdentifier(String prefix) {
    return prefix + SEQS.computeIfAbsent(prefix, s -> new AtomicInteger(0)).incrementAndGet();
  }

  public static int countOccurrences(char c, String s) {
    return CharMatcher.is(c).countIn(s);
  }

  /**
   * CSVC Escapes and normalizes the given path.
   *
   * @param value the value to escape.
   * @return the escaped value.
   * @see #escapeUserInput(String)
   */
  public static String escapeUserInput(Path value) {
    return escapeUserInput(value.normalize().toAbsolutePath().toString());
  }

  /**
   * Escapes and normalizes the given URL.
   *
   * @param value the value to escape.
   * @return the escaped value.
   * @see #escapeUserInput(String)
   */
  public static String escapeUserInput(URL value) {
    return escapeUserInput(value.toExternalForm());
  }

  /**
   * Escapes the given string and returns an escaped string compliant with Json syntax for quoted
   * strings. Useful in tests to escape paths and other variables as if they were provided by the
   * user through the command line.
   *
   * @param value the value to escape.
   * @return the escaped value.
   */
  public static String escapeUserInput(String value) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < value.length(); ++i) {
      char c = value.charAt(i);
      switch (c) {
        case '"':
          sb.append("\\\"");
          break;
        case '\\':
          sb.append("\\\\");
          break;
        case '\n':
          sb.append("\\n");
          break;
        case '\b':
          sb.append("\\b");
          break;
        case '\f':
          sb.append("\\f");
          break;
        case '\r':
          sb.append("\\r");
          break;
        case '\t':
          sb.append("\\t");
          break;
        default:
          if (isC0Control(c)) {
            sb.append(String.format("\\u%04x", (int) c));
          } else {
            sb.append(c);
          }
      }
    }
    return sb.toString();
  }

  private static boolean isC0Control(int codepoint) {
    return (codepoint >= 0x0000 && codepoint <= 0x001F);
  }
}
