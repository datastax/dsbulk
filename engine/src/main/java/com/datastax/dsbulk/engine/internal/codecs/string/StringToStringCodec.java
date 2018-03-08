/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import java.util.List;

public class StringToStringCodec extends ConvertingCodec<String, String> {

  private final List<String> nullWords;

  public StringToStringCodec(TypeCodec<String> innerCodec, List<String> nullWords) {
    super(innerCodec, String.class);
    this.nullWords = nullWords;
  }

  @Override
  public String convertFrom(String s) {
    if (s == null || s.isEmpty() || nullWords.contains(s)) {
      return null;
    }
    return s;
  }

  @Override
  public String convertTo(String value) {
    if (value == null) {
      return nullWords.isEmpty() ? null : nullWords.get(0);
    }
    return value;
  }
}
