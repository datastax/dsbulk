/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.commons.internal.utils;

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
