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

  private final List<String> nullStrings;

  public StringToDurationCodec(List<String> nullStrings) {
    super(duration(), String.class);
    this.nullStrings = nullStrings;
  }

  @Override
  public Duration externalToInternal(String s) {
    if (s == null || s.isEmpty() || nullStrings.contains(s)) {
      return null;
    }
    return Duration.from(s);
  }

  @Override
  public String internalToExternal(Duration value) {
    if (value == null) {
      return nullStrings.isEmpty() ? null : nullStrings.get(0);
    }
    return value.toString();
  }
}
