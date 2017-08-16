/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import com.datastax.driver.core.TypeCodec;
import com.google.common.base.Splitter;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class StringToMapCodec<K, V> extends ConvertingCodec<String, Map<K, V>> {

  private final ConvertingCodec<String, K> keyCodec;
  private final ConvertingCodec<String, V> valueCodec;

  private final String delimiter;
  private final String keyValueSeparator;

  StringToMapCodec(
      TypeCodec<Map<K, V>> collectionCodec,
      ConvertingCodec<String, K> keyCodec,
      ConvertingCodec<String, V> valueCodec,
      String delimiter,
      String keyValueSeparator) {
    super(collectionCodec, String.class);
    this.valueCodec = valueCodec;
    this.delimiter = delimiter;
    this.keyCodec = keyCodec;
    this.keyValueSeparator = keyValueSeparator;
  }

  @Override
  protected String convertTo(Map<K, V> map) {
    if (map == null) {
      return null;
    }
    StringBuilder sb = new StringBuilder();
    for (Iterator<Map.Entry<K, V>> it = map.entrySet().iterator(); it.hasNext(); ) {
      Map.Entry<K, V> entry = it.next();
      String key = keyCodec.convertTo(entry.getKey());
      String value = valueCodec.convertTo(entry.getValue());
      sb.append(key == null ? "" : key)
          .append(keyValueSeparator)
          .append(value == null ? "" : value);
      if (it.hasNext()) {
        sb.append(delimiter);
      }
    }
    return sb.toString();
  }

  @Override
  protected Map<K, V> convertFrom(String s) {
    if (s == null || s.isEmpty()) {
      return null;
    }
    Map<String, String> raw =
        Splitter.on(delimiter).trimResults().withKeyValueSeparator(keyValueSeparator).split(s);
    Map<K, V> map = new LinkedHashMap<>(raw.size());
    for (Map.Entry<String, String> entry : raw.entrySet()) {
      map.put(
          keyCodec.convertFrom(CodecUtils.trimToNull(entry.getKey())),
          valueCodec.convertFrom(CodecUtils.trimToNull(entry.getValue())));
    }
    return map;
  }
}
