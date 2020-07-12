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
package com.datastax.oss.dsbulk.codecs.jdk.temporal;

import static com.datastax.oss.dsbulk.codecs.api.CommonConversionContext.EPOCH;
import static com.datastax.oss.dsbulk.codecs.api.CommonConversionContext.TIME_UUID_GENERATOR;
import static com.datastax.oss.dsbulk.codecs.api.CommonConversionContext.TIME_ZONE;
import static com.datastax.oss.dsbulk.codecs.jdk.JdkCodecUtils.isTemporal;
import static com.datastax.oss.dsbulk.codecs.jdk.JdkCodecUtils.isUUID;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.codecs.api.ConversionContext;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecProvider;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Instant;
import java.time.temporal.Temporal;
import java.time.temporal.TemporalAccessor;
import java.util.Date;
import java.util.Optional;
import java.util.UUID;

public class TemporalConvertingCodecsProvider implements ConvertingCodecProvider {

  private static final GenericType<?> TEMPORAL_GENERIC_TYPE = GenericType.of(Temporal.class);
  private static final GenericType<Date> DATE_GENERIC_TYPE = GenericType.of(Date.class);

  @NonNull
  @Override
  public Optional<ConvertingCodec<?, ?>> maybeProvide(
      @NonNull DataType cqlType,
      @NonNull GenericType<?> externalJavaType,
      @NonNull ConvertingCodecFactory codecFactory,
      boolean rootCodec) {

    ConversionContext context = codecFactory.getContext();

    if (externalJavaType.isSubtypeOf(TEMPORAL_GENERIC_TYPE) && isTemporal(cqlType)) {
      @SuppressWarnings("unchecked")
      Class<Temporal> fromTemporalType = (Class<Temporal>) externalJavaType.getRawType();
      if (cqlType == DataTypes.DATE) {
        return Optional.of(
            new TemporalToTemporalCodec<>(
                fromTemporalType,
                TypeCodecs.DATE,
                context.getAttribute(TIME_ZONE),
                context.getAttribute(EPOCH)));
      }
      if (cqlType == DataTypes.TIME) {
        return Optional.of(
            new TemporalToTemporalCodec<>(
                fromTemporalType,
                TypeCodecs.TIME,
                context.getAttribute(TIME_ZONE),
                context.getAttribute(EPOCH)));
      }
      if (cqlType == DataTypes.TIMESTAMP) {
        return Optional.of(
            new TemporalToTemporalCodec<>(
                fromTemporalType,
                TypeCodecs.TIMESTAMP,
                context.getAttribute(TIME_ZONE),
                context.getAttribute(EPOCH)));
      }
    }

    if (externalJavaType.isSubtypeOf(TEMPORAL_GENERIC_TYPE) && isUUID(cqlType)) {
      TypeCodec<UUID> uuidCodec = codecFactory.getCodecRegistry().codecFor(cqlType);
      @SuppressWarnings({"unchecked", "rawtypes"})
      TemporalToTemporalCodec<TemporalAccessor, Instant> instantCodec =
          (TemporalToTemporalCodec)
              codecFactory.createConvertingCodec(DataTypes.TIMESTAMP, externalJavaType, false);
      return Optional.of(
          new TemporalToUUIDCodec<>(
              uuidCodec, instantCodec, context.getAttribute(TIME_UUID_GENERATOR)));
    }

    if (externalJavaType.isSubtypeOf(DATE_GENERIC_TYPE) && isTemporal(cqlType)) {
      if (cqlType == DataTypes.DATE) {
        return Optional.of(
            new DateToTemporalCodec<>(
                Date.class, TypeCodecs.DATE, context.getAttribute(TIME_ZONE)));
      }
      if (cqlType == DataTypes.TIME) {
        return Optional.of(
            new DateToTemporalCodec<>(
                Date.class, TypeCodecs.TIME, context.getAttribute(TIME_ZONE)));
      }
      if (cqlType == DataTypes.TIMESTAMP) {
        return Optional.of(
            new DateToTemporalCodec<>(
                Date.class, TypeCodecs.TIMESTAMP, context.getAttribute(TIME_ZONE)));
      }
    }

    if (externalJavaType.isSubtypeOf(DATE_GENERIC_TYPE) && isUUID(cqlType)) {
      TypeCodec<UUID> uuidCodec = codecFactory.getCodecRegistry().codecFor(cqlType);
      @SuppressWarnings({"unchecked", "rawtypes"})
      DateToTemporalCodec<Date, Instant> instantCodec =
          (DateToTemporalCodec)
              codecFactory.createConvertingCodec(DataTypes.TIMESTAMP, externalJavaType, false);
      return Optional.of(
          new DateToUUIDCodec<>(
              uuidCodec, instantCodec, context.getAttribute(TIME_UUID_GENERATOR)));
    }

    return Optional.empty();
  }
}
