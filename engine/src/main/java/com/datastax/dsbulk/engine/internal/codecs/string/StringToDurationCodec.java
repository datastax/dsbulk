/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.Duration;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;

public class StringToDurationCodec extends ConvertingCodec<String, Duration> {

  public static final StringToDurationCodec INSTANCE = new StringToDurationCodec();

  private StringToDurationCodec() {
    super(duration(), String.class);
  }

  @Override
  public Duration convertFrom(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    return Duration.from(s);
  }

  @Override
  public String convertTo(Duration value) {
    if (value == null) {
      return null;
    }
    return value.toString();
  }
}
