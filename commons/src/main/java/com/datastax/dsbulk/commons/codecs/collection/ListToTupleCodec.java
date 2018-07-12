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
import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import java.util.ArrayList;
import java.util.List;

public class ListToTupleCodec<E> extends ConvertingCodec<List<E>, TupleValue> {
  private final List<ConvertingCodec<E, Object>> elementCodecs;
  private final TupleType definition;

  public ListToTupleCodec(
      Class<List<E>> javaType,
      TypeCodec<TupleValue> targetCodec,
      List<ConvertingCodec<E, Object>> elementCodecs) {
    super(targetCodec, javaType);
    this.elementCodecs = elementCodecs;
    definition = (TupleType) targetCodec.getCqlType();
  }

  @Override
  public TupleValue externalToInternal(List<E> external) {
    if (external == null || external.isEmpty()) {
      return null;
    }

    int size = definition.getComponentTypes().size();
    if (external.size() != size) {
      throw new IllegalArgumentException(
          String.format("Expecting %d elements, got %d", size, external.size()));
    }

    TupleValue tuple = definition.newValue();

    int idx = 0;
    for (E item : external) {
      ConvertingCodec<E, Object> eltCodec = elementCodecs.get(idx);
      Object o = eltCodec.externalToInternal(item);
      tuple.set(idx, o, eltCodec.getInternalJavaType());
      idx++;
    }
    return tuple;
  }

  @Override
  public List<E> internalToExternal(TupleValue tuple) {
    if (tuple == null) {
      return null;
    }
    List<E> result = new ArrayList<>();
    int size = definition.getComponentTypes().size();
    for (int i = 0; i < size; i++) {
      ConvertingCodec<E, Object> eltCodec = elementCodecs.get(i);
      Object o = tuple.get(i, eltCodec.getInternalJavaType());
      result.add(eltCodec.internalToExternal(o));
    }
    return result;
  }
}
