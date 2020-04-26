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
package com.datastax.oss.dsbulk.codecs.jdk.collection;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.dsbulk.codecs.ConvertingCodec;
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
      tuple = tuple.set(idx, o, eltCodec.getInternalJavaType());
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
