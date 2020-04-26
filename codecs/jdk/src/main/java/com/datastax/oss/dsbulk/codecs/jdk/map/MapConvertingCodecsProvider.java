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
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.MapType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.codecs.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecProvider;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.lang.reflect.ParameterizedType;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class MapConvertingCodecsProvider implements ConvertingCodecProvider {

  @NonNull
  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public Optional<ConvertingCodec<?, ?>> maybeProvide(
      @NonNull DataType cqlType,
      @NonNull GenericType<?> externalJavaType,
      @NonNull ConvertingCodecFactory codecFactory,
      boolean rootCodec) {

    CodecRegistry codecRegistry = codecFactory.getCodecRegistry();

    if (externalJavaType.isSubtypeOf(GenericType.of(Map.class))
        && externalJavaType.getType() instanceof ParameterizedType
        && ((ParameterizedType) externalJavaType.getType()).getActualTypeArguments().length == 2) {
      GenericType<?> keyType =
          GenericType.of(
              ((ParameterizedType) externalJavaType.getType()).getActualTypeArguments()[0]);
      GenericType<?> valueType =
          GenericType.of(
              ((ParameterizedType) externalJavaType.getType()).getActualTypeArguments()[1]);
      if (cqlType instanceof MapType) {
        TypeCodec<?> typeCodec = codecRegistry.codecFor(cqlType);
        ConvertingCodec<?, ?> keyConvertingCodec =
            codecFactory.createConvertingCodec(((MapType) cqlType).getKeyType(), keyType, false);
        ConvertingCodec<?, ?> valueConvertingCodec =
            codecFactory.createConvertingCodec(
                ((MapType) cqlType).getValueType(), valueType, false);
        MapToMapCodec codec =
            new MapToMapCodec(
                externalJavaType.getRawType(), typeCodec, keyConvertingCodec, valueConvertingCodec);
        return Optional.of(codec);
      } else if (cqlType instanceof UserDefinedType) {
        TypeCodec<UdtValue> udtCodec = codecRegistry.codecFor(cqlType);
        ImmutableMap.Builder<CqlIdentifier, ConvertingCodec<?, Object>> fieldCodecs =
            new ImmutableMap.Builder<>();
        List<CqlIdentifier> fieldNames = ((UserDefinedType) cqlType).getFieldNames();
        List<DataType> fieldTypes = ((UserDefinedType) cqlType).getFieldTypes();
        ConvertingCodec<?, String> keyCodec =
            codecFactory.createConvertingCodec(DataTypes.TEXT, keyType, false);
        assert (fieldNames.size() == fieldTypes.size());
        for (int idx = 0; idx < fieldNames.size(); idx++) {
          CqlIdentifier fieldName = fieldNames.get(idx);
          DataType fieldType = fieldTypes.get(idx);
          ConvertingCodec<?, Object> fieldCodec =
              codecFactory.createConvertingCodec(fieldType, valueType, false);
          fieldCodecs.put(fieldName, fieldCodec);
        }
        MapToUDTCodec codec =
            new MapToUDTCodec(
                externalJavaType.getRawType(), udtCodec, keyCodec, fieldCodecs.build());
        return Optional.of(codec);
      }
    }
    return Optional.empty();
  }
}
