/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.string;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import java.util.List;

public abstract class StringConvertingCodec<T> extends ConvertingCodec<String, T> {

  private final List<String> nullStrings;

  protected StringConvertingCodec(TypeCodec<T> targetCodec, List<String> nullStrings) {
    super(targetCodec, String.class);
    this.nullStrings = nullStrings;
  }

  protected boolean isNull(String s) {
    return s == null || nullStrings.contains(s);
  }

  protected boolean isNullOrEmpty(String s) {
    return isNull(s) || s.isEmpty();
  }

  protected String nullString() {
    return nullStrings.isEmpty() ? "" : nullStrings.get(0);
  }
}
