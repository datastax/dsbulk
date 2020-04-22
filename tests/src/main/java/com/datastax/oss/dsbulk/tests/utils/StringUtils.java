/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.tests.utils;

import com.datastax.oss.driver.shaded.guava.common.base.CharMatcher;
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
   * @param prefix the prefix to use.
   * @return a unique CQL identifier.
   */
  public static String uniqueIdentifier(String prefix) {
    return prefix + SEQS.computeIfAbsent(prefix, s -> new AtomicInteger(0)).incrementAndGet();
  }

  public static int countOccurrences(char c, String s) {
    return CharMatcher.is(c).countIn(s);
  }

  /**
   * Quotes the given path as a Json string.
   *
   * @param value the value to quote.
   * @return the quoted value.
   * @see #quoteJson(String)
   */
  public static String quoteJson(Path value) {
    return quoteJson(value.normalize().toAbsolutePath().toString());
  }

  /**
   * Quotes the given URL as a Json string.
   *
   * @param value the value to quote.
   * @return the quoted value.
   * @see #quoteJson(String)
   */
  public static String quoteJson(URL value) {
    return quoteJson(value.toExternalForm());
  }

  /**
   * Quotes the given string as a Json string.
   *
   * @param value the value to escape.
   * @return the quoted value.
   */
  public static String quoteJson(String value) {
    StringBuilder sb = new StringBuilder("\"");
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
    return sb.append("\"").toString();
  }

  private static boolean isC0Control(int codepoint) {
    return (codepoint >= 0x0000 && codepoint <= 0x001F);
  }
}
