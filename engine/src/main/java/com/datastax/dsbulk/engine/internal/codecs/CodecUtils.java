/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs;

import org.jetbrains.annotations.Nullable;

@SuppressWarnings("Duplicates")
public final class CodecUtils {

  @Nullable
  public static String trimToNull(String s) {
    if (s == null) {
      return null;
    }
    s = s.trim();
    return s.isEmpty() ? null : s;
  }
}
