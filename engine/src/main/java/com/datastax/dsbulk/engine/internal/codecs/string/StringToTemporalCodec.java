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
import com.datastax.dsbulk.engine.internal.codecs.util.CodecUtils;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.List;

public abstract class StringToTemporalCodec<T extends TemporalAccessor>
    extends ConvertingCodec<String, T> {

  final DateTimeFormatter temporalFormat;
  final List<String> nullStrings;

  StringToTemporalCodec(
      TypeCodec<T> targetCodec, DateTimeFormatter temporalFormat, List<String> nullStrings) {
    super(targetCodec, String.class);
    this.temporalFormat = temporalFormat;
    this.nullStrings = nullStrings;
  }

  @Override
  public String internalToExternal(T value) {
    if (value == null) {
      return nullStrings.isEmpty() ? null : nullStrings.get(0);
    }
    return temporalFormat.format(value);
  }

  TemporalAccessor parseTemporalAccessor(String s) {
    if (s == null || s.isEmpty() || nullStrings.contains(s)) {
      return null;
    }
    return CodecUtils.parseTemporal(s, temporalFormat);
  }
}
