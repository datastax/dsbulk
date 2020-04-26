/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.codecs.jdk.map;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.dsbulk.codecs.ConvertingCodec;
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
      value = value.set(udtFieldName, o, fieldCodec.getInternalJavaType());
    }
    return value;
  }

  @Override
  public Map<EK, EV> internalToExternal(UdtValue internal) {
    if (internal == null) {
      return null;
    }
    Map<EK, EV> result = new HashMap<>();
    for (CqlIdentifier name : definition.getFieldNames()) {
      EK key = keyCodec.internalToExternal(name.asInternal());
      ConvertingCodec<EV, Object> eltCodec = fieldCodecs.get(name);
      Object o = internal.get(name, eltCodec.getInternalJavaType());
      EV value = eltCodec.internalToExternal(o);
      result.put(key, value);
    }
    return result;
  }
}
