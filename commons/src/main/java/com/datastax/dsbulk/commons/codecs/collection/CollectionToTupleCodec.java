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
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.util.Collection;
import java.util.List;

public class CollectionToTupleCodec<E, EXTERNAL extends Collection<E>>
    extends ConvertingCodec<EXTERNAL, TupleValue> {
  private List<TypeCodec<?>> elementCodecs;
  private final TupleType definition;

  public CollectionToTupleCodec(
      Class<EXTERNAL> javaType,
      TypeCodec<TupleValue> targetCodec,
      List<TypeCodec<?>> elementCodecs) {
    super(targetCodec, javaType);
    this.elementCodecs = elementCodecs;
    definition = (TupleType) targetCodec.getCqlType();
  }

  @SuppressWarnings("unchecked")
  @Override
  public TupleValue externalToInternal(EXTERNAL external) {
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
      TypeCodec<?> eltCodec = elementCodecs.get(idx);
      Object o;
      if (!(eltCodec instanceof ConvertingCodec)) {
        tuple.set(idx, item, (GenericType<E>) eltCodec.getJavaType());
      } else {
        o = ((ConvertingCodec) eltCodec).externalToInternal(item);
        tuple.set(idx, o, ((ConvertingCodec) eltCodec).getInternalJavaType());
      }
      idx++;
    }
    return tuple;
  }

  @SuppressWarnings("unchecked")
  @Override
  public EXTERNAL internalToExternal(TupleValue internal) {
    // TODO
    return null;
  }
}
