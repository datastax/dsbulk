/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.map;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MapToUDTCodec<EK, EV> extends ConvertingCodec<Map<EK, EV>, UdtValue> {
  private final Map<CqlIdentifier, ConvertingCodec<EV, Object>> fieldCodecs;
  private final ConvertingCodec<EK, String> keyCodec;
  private final UserDefinedType definition;

  public MapToUDTCodec(
      Class<Map<EK, EV>> javaType,
      TypeCodec<UdtValue> targetCodec,
      ConvertingCodec<EK, String> keyCodec,
      Map<CqlIdentifier, ConvertingCodec<EV, Object>> fieldCodecs) {
    super(targetCodec, javaType);
    this.fieldCodecs = fieldCodecs;
    this.keyCodec = keyCodec;
    definition = (UserDefinedType) targetCodec.getCqlType();
  }

  @SuppressWarnings("unchecked")
  @Override
  public UdtValue externalToInternal(Map<EK, EV> external) {
    if (external == null || external.isEmpty()) {
      return null;
    }

    int size = definition.getFieldNames().size();
    if (external.size() != size) {
      throw new IllegalArgumentException(
          String.format("Expecting %d fields, got %d", size, external.size()));
    }

    UdtValue value = definition.newValue();
    List<CqlIdentifier> fieldNames = definition.getFieldNames();
    for (CqlIdentifier udtFieldName : fieldNames) {
      EK mapKey = keyCodec.internalToExternal(udtFieldName.asInternal());
      if (!external.containsKey(mapKey)) {
        throw new IllegalArgumentException(
            String.format(
                "Field %s in UDT %s not found in input map", udtFieldName, definition.getName()));
      }
      ConvertingCodec<EV, Object> fieldCodec = fieldCodecs.get(udtFieldName);
      Object o = fieldCodec.externalToInternal(external.get(mapKey));
      value.set(udtFieldName, o, fieldCodec.getInternalJavaType());
    }
    return value;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Map<EK, EV> internalToExternal(UdtValue internal) {
    if (internal == null) {
      return null;
    }

    Map<EK, EV> result = new HashMap<>();
    for (CqlIdentifier name : definition.getFieldNames()) {
      ConvertingCodec<EV, Object> eltCodec = fieldCodecs.get(name);
      Object o = internal.get(name, eltCodec.getInternalJavaType());
      EV out = eltCodec.internalToExternal(o);
      result.put(keyCodec.internalToExternal(name.asInternal()), out);
    }
    return result;
  }
}
