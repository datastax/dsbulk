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

import static com.datastax.oss.dsbulk.codecs.jdk.JdkCodecUtils.isCollection;

import com.datastax.oss.driver.api.core.data.TupleValue;
import com.datastax.oss.driver.api.core.data.UdtValue;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.ListType;
import com.datastax.oss.driver.api.core.type.SetType;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecProvider;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class CollectionConvertingCodecsProvider implements ConvertingCodecProvider {

  @NonNull
  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public Optional<ConvertingCodec<?, ?>> maybeProvide(
      @NonNull DataType cqlType,
      @NonNull GenericType<?> externalJavaType,
      @NonNull ConvertingCodecFactory codecFactory,
      boolean rootCodec) {

    CodecRegistry codecRegistry = codecFactory.getCodecRegistry();

    if (externalJavaType.isSubtypeOf(GenericType.of(List.class))
        && externalJavaType.getType() instanceof ParameterizedType
        && ((ParameterizedType) externalJavaType.getType()).getActualTypeArguments().length == 1) {
      GenericType<?> componentType =
          GenericType.of(
              ((ParameterizedType) externalJavaType.getType()).getActualTypeArguments()[0]);
      if (cqlType instanceof UserDefinedType) {
        TypeCodec<UdtValue> udtCodec = codecRegistry.codecFor(cqlType);
        ImmutableList.Builder<ConvertingCodec> eltCodecs = new ImmutableList.Builder<>();
        for (DataType eltType : ((UserDefinedType) cqlType).getFieldTypes()) {
          ConvertingCodec eltCodec =
              codecFactory.createConvertingCodec(eltType, componentType, false);
          eltCodecs.add(eltCodec);
        }
        ListToUDTCodec codec =
            new ListToUDTCodec(externalJavaType.getRawType(), udtCodec, eltCodecs.build());
        return Optional.of(codec);
      } else if (cqlType instanceof TupleType) {
        TypeCodec<TupleValue> tupleCodec = codecRegistry.codecFor(cqlType);
        ImmutableList.Builder<ConvertingCodec> eltCodecs = new ImmutableList.Builder<>();
        for (DataType eltType : ((TupleType) cqlType).getComponentTypes()) {
          ConvertingCodec eltCodec =
              codecFactory.createConvertingCodec(eltType, componentType, false);
          eltCodecs.add(eltCodec);
        }
        ListToTupleCodec codec =
            new ListToTupleCodec(externalJavaType.getRawType(), tupleCodec, eltCodecs.build());
        return Optional.of(codec);
      }
    }

    if (externalJavaType.isSubtypeOf(GenericType.of(Collection.class))
        && externalJavaType.getType() instanceof ParameterizedType
        && ((ParameterizedType) externalJavaType.getType()).getActualTypeArguments().length == 1) {
      GenericType<?> componentType =
          GenericType.of(
              ((ParameterizedType) externalJavaType.getType()).getActualTypeArguments()[0]);
      if (isCollection(cqlType)) {
        TypeCodec<?> typeCodec = codecRegistry.codecFor(cqlType);
        ConvertingCodec elementCodec = null;
        Supplier<? extends Collection<?>> collectionCreator;
        if (cqlType instanceof SetType) {
          elementCodec =
              codecFactory.createConvertingCodec(
                  ((SetType) cqlType).getElementType(), componentType, false);
        } else if (cqlType instanceof ListType) {
          elementCodec =
              codecFactory.createConvertingCodec(
                  ((ListType) cqlType).getElementType(), componentType, false);
        }
        if (cqlType instanceof SetType) {
          collectionCreator = HashSet::new;
        } else {
          collectionCreator = ArrayList::new;
        }
        Class<? extends Collection<?>> collType =
            (Class<? extends Collection<?>>) externalJavaType.getRawType();
        CollectionToCollectionCodec codec =
            new CollectionToCollectionCodec(collType, typeCodec, elementCodec, collectionCreator);
        return Optional.of(codec);
      }
    }
    return Optional.empty();
  }
}
