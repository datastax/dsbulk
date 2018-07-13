/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.collection;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import java.util.ArrayList;
import java.util.List;

public class ListToUDTCodec<E> extends ConvertingCodec<List<E>, UdtValue> {
  private final List<ConvertingCodec<E, Object>> elementCodecs;
  private final UserDefinedType definition;

  public ListToUDTCodec(
      Class<List<E>> javaType,
      TypeCodec<UdtValue> targetCodec,
      List<ConvertingCodec<E, Object>> elementCodecs) {
    super(targetCodec, javaType);
    this.elementCodecs = elementCodecs;
    definition = (UserDefinedType) targetCodec.getCqlType();
  }

  @Override
  public UdtValue externalToInternal(List<E> external) {
    if (external == null || external.isEmpty()) {
      return null;
    }

    int size = definition.getFieldNames().size();
    if (external.size() != size) {
      throw new IllegalArgumentException(
          String.format("Expecting %d elements, got %d", size, external.size()));
    }

    UdtValue value = definition.newValue();

    int idx = 0;
    for (E item : external) {
      ConvertingCodec<E, Object> eltCodec = elementCodecs.get(idx);
      Object o = eltCodec.externalToInternal(item);
      value.set(idx, o, eltCodec.getInternalJavaType());
      idx++;
    }
    return value;
  }

  @Override
  public List<E> internalToExternal(UdtValue tuple) {
    if (tuple == null) {
      return null;
    }
    List<E> result = new ArrayList<>();
    int size = definition.getFieldNames().size();
    for (int i = 0; i < size; i++) {
      ConvertingCodec<E, Object> eltCodec = elementCodecs.get(i);
      Object o = tuple.get(i, eltCodec.getInternalJavaType());
      result.add(eltCodec.internalToExternal(o));
    }
    return result;
  }
}
