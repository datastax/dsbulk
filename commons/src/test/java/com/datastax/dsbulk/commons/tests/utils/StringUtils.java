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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

/** */
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
}
