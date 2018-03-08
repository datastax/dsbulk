/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.Duration;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import java.util.List;

public class StringToDurationCodec extends ConvertingCodec<String, Duration> {

  private final List<String> nullWords;

  public StringToDurationCodec(List<String> nullWords) {
    super(duration(), String.class);
    this.nullWords = nullWords;
  }

  @Override
  public Duration convertFrom(String s) {
    if (s == null || s.isEmpty() || nullWords.contains(s)) {
      return null;
    }
    return Duration.from(s);
  }

  @Override
  public String convertTo(Duration value) {
    if (value == null) {
      return nullWords.isEmpty() ? null : nullWords.get(0);
    }
    return value.toString();
  }
}
