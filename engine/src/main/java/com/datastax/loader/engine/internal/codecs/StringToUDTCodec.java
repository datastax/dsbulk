/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import static com.datastax.loader.engine.internal.codecs.CodecUtils.trimToNull;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.UserType;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import com.google.common.base.Splitter;
import java.util.Iterator;
import java.util.Map;

public class StringToUDTCodec extends ConvertingCodec<String, UDTValue> {

  private final Map<String, ConvertingCodec<String, Object>> fieldCodecs;
  private final UserType definition;

  private final String delimiter;
  private final String keyValueSeparator;

  StringToUDTCodec(
      TypeCodec<UDTValue> udtCodec,
      Map<String, ConvertingCodec<String, Object>> fieldCodecs,
      String delimiter,
      String keyValueSeparator) {
    super(udtCodec, String.class);
    this.fieldCodecs = fieldCodecs;
    this.delimiter = delimiter;
    this.keyValueSeparator = keyValueSeparator;
    definition = (UserType) udtCodec.getCqlType();
  }

  @Override
  protected String convertTo(UDTValue value) {
    if (value == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (Iterator<UserType.Field> it = definition.iterator(); it.hasNext(); ) {
      UserType.Field field = it.next();
      String name = field.getName();
      ConvertingCodec<String, Object> eltCodec = fieldCodecs.get(name);
      Object o = value.get(name, eltCodec.getTargetJavaType());
      String s = eltCodec.convertTo(o);
      sb.append(name).append(keyValueSeparator).append(s == null ? "" : s);
      if (it.hasNext()) {
        sb.append(delimiter);
      }
    }
    return sb.toString();
  }

  @Override
  protected UDTValue convertFrom(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    Map<String, String> map =
        Splitter.on(delimiter).trimResults().withKeyValueSeparator(keyValueSeparator).split(s);
    UDTValue value = definition.newValue();
    for (Map.Entry<String, String> entry : map.entrySet()) {
      String name = trimToNull(entry.getKey());
      if (definition.getFieldType(name) == null) {
        throw new InvalidTypeException(
            String.format("Unknown field %s in UDT %s", name, definition.getName()));
      }
      ConvertingCodec<String, Object> fieldCodec = fieldCodecs.get(name);
      Object o = fieldCodec.convertFrom(trimToNull(entry.getValue()));
      value.set(name, o, fieldCodec.getTargetJavaType());
    }
    return value;
  }
}
