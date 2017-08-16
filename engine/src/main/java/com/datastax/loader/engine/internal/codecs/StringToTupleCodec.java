/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import static com.datastax.loader.engine.internal.codecs.CodecUtils.trimToNull;

import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TupleValue;
import com.datastax.driver.core.TypeCodec;
import com.google.common.base.Splitter;
import java.util.List;

public class StringToTupleCodec extends ConvertingCodec<String, TupleValue> {

  private final TupleType definition;
  private final List<ConvertingCodec<String, Object>> eltCodecs;
  private final String delimiter;

  public StringToTupleCodec(
      TypeCodec<TupleValue> tupleCodec,
      List<ConvertingCodec<String, Object>> eltCodecs,
      String delimiter) {
    super(tupleCodec, String.class);
    this.eltCodecs = eltCodecs;
    this.delimiter = delimiter;
    definition = (TupleType) tupleCodec.getCqlType();
  }

  @Override
  protected String convertTo(TupleValue value) {
    if (value == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    int size = definition.getComponentTypes().size();
    for (int i = 0; i < size; i++) {
      if (i > 0) {
        sb.append(delimiter);
      }
      ConvertingCodec<String, Object> eltCodec = eltCodecs.get(i);
      Object o = value.get(i, eltCodec.getTargetJavaType());
      String s = eltCodec.convertTo(o);
      if (s != null) {
        sb.append(s);
      }
    }
    return sb.toString();
  }

  @Override
  protected TupleValue convertFrom(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    List<String> elts = Splitter.on(delimiter).trimResults().splitToList(s);
    TupleValue value = definition.newValue();
    for (int i = 0; i < definition.getComponentTypes().size(); i++) {
      ConvertingCodec<String, Object> eltCodec = eltCodecs.get(i);
      Object o = eltCodec.convertFrom(trimToNull(elts.get(i)));
      value.set(i, o, eltCodec.getTargetJavaType());
    }
    return value;
  }
}
