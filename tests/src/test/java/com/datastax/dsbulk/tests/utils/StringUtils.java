/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.tests.utils;

import java.util.concurrent.atomic.AtomicInteger;

/** */
public class StringUtils {

  private static final AtomicInteger SEQ = new AtomicInteger(0);

  /**
   * Generates a unique CQL identifier with prefix "test".
   *
   * @return a unique CQL identifier.
   */
  public static String uniqueIdentifier() {
    return "test" + SEQ.incrementAndGet();
  }
}
