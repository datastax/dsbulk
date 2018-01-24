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

public abstract class StringToTemporalCodec<T extends TemporalAccessor>
    extends ConvertingCodec<String, T> {

  final DateTimeFormatter parser;

  StringToTemporalCodec(TypeCodec<T> targetCodec, DateTimeFormatter parser) {
    super(targetCodec, String.class);
    this.parser = parser;
  }

  TemporalAccessor parseAsTemporalAccessor(String s) {
    return CodecUtils.parseTemporal(s, parser);
  }

  @Override
  public String convertTo(T value) {
    if (value == null) {
      return null;
    }
    return parser.format(value);
  }
}
