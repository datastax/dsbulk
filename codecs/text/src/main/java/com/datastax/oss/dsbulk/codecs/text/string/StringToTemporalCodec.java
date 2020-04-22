/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.codecs.text.string;

import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.dsbulk.codecs.util.TemporalFormat;
import java.time.temporal.TemporalAccessor;
import java.util.List;

public abstract class StringToTemporalCodec<T extends TemporalAccessor>
    extends StringConvertingCodec<T> {

  final TemporalFormat temporalFormat;

  StringToTemporalCodec(
      TypeCodec<T> targetCodec, TemporalFormat temporalFormat, List<String> nullStrings) {
    super(targetCodec, nullStrings);
    this.temporalFormat = temporalFormat;
  }

  @Override
  public String internalToExternal(T value) {
    if (value == null) {
      return nullString();
    }
    return temporalFormat.format(value);
  }

  TemporalAccessor parseTemporalAccessor(String s) {
    if (isNullOrEmpty(s)) {
      return null;
    }
    return temporalFormat.parse(s);
  }
}
