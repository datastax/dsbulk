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
package com.datastax.oss.dsbulk.codecs.jdk.number;

import static com.datastax.oss.dsbulk.codecs.CommonConversionContext.BOOLEAN_NUMBERS;
import static com.datastax.oss.dsbulk.codecs.CommonConversionContext.EPOCH;
import static com.datastax.oss.dsbulk.codecs.CommonConversionContext.NUMBER_FORMAT;
import static com.datastax.oss.dsbulk.codecs.CommonConversionContext.TIME_UNIT;
import static com.datastax.oss.dsbulk.codecs.CommonConversionContext.TIME_UUID_GENERATOR;
import static com.datastax.oss.dsbulk.codecs.jdk.JdkCodecUtils.isNumeric;
import static com.datastax.oss.dsbulk.codecs.jdk.JdkCodecUtils.isUUID;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.codecs.ConversionContext;
import com.datastax.oss.dsbulk.codecs.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecProvider;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Optional;
import java.util.UUID;

public class NumericConvertingCodecsProvider implements ConvertingCodecProvider {
  @NonNull
  @Override
  public Optional<ConvertingCodec<?, ?>> maybeProvide(
      @NonNull DataType cqlType,
      @NonNull GenericType<?> externalJavaType,
      @NonNull ConvertingCodecFactory codecFactory,
      boolean rootCodec) {

    CodecRegistry codecRegistry = codecFactory.getCodecRegistry();
    ConversionContext context = codecFactory.getContext();

    if (externalJavaType.isSubtypeOf(GenericType.of(Number.class))) {
      @SuppressWarnings("unchecked")
      Class<Number> numberType = (Class<Number>) externalJavaType.getRawType();
      if (isNumeric(cqlType)) {
        TypeCodec<Number> typeCodec = codecRegistry.codecFor(cqlType);
        return Optional.of(new NumberToNumberCodec<>(numberType, typeCodec));
      }
      if (cqlType == DataTypes.TIMESTAMP) {
        return Optional.of(
            new NumberToInstantCodec<>(
                numberType, context.getAttribute(TIME_UNIT), context.getAttribute(EPOCH)));
      }
      if (isUUID(cqlType)) {
        TypeCodec<UUID> uuidCodec = codecRegistry.codecFor(cqlType);
        NumberToInstantCodec<Number> instantCodec =
            new NumberToInstantCodec<>(
                numberType, context.getAttribute(TIME_UNIT), context.getAttribute(EPOCH));
        return Optional.of(
            new NumberToUUIDCodec<>(
                uuidCodec, instantCodec, context.getAttribute(TIME_UUID_GENERATOR)));
      }
      if (cqlType == DataTypes.BOOLEAN) {
        return Optional.of(
            new NumberToBooleanCodec<>(numberType, context.getAttribute(BOOLEAN_NUMBERS)));
      }
      if (cqlType == DataTypes.TEXT || cqlType == DataTypes.ASCII) {
        // TODO: Consider a more general solution where we have an "invert" codec that
        // we can apply on a regular codec (e.g. StringToXXX) to convert from the
        // "other" type to String. Such a mechanism wouldn't be restricted to converting
        // number types to String, but rather any java type that has a corresponding cql type.
        return Optional.of(
            new NumberToStringCodec<>(numberType, context.getAttribute(NUMBER_FORMAT)));
      }
    }
    return Optional.empty();
  }
}
