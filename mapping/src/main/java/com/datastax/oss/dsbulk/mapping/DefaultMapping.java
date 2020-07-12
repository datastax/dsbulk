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
package com.datastax.oss.dsbulk.mapping;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSetMultimap;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.api.writetime.WriteTimeCodec;
import com.datastax.oss.dsbulk.connectors.api.Field;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.time.Instant;
import java.util.Set;

public class DefaultMapping implements Mapping {

  private final ImmutableSetMultimap<Field, CQLWord> fieldsToVariables;
  private final ImmutableSetMultimap<CQLWord, Field> variablesToFields;
  private final ConvertingCodecFactory codecFactory;
  private final Cache<MappingToken, TypeCodec<?>> variablesToCodecs;
  private final ImmutableSet<CQLWord> writeTimeVariables;

  public DefaultMapping(
      ImmutableSetMultimap<Field, CQLWord> fieldsToVariables,
      ConvertingCodecFactory codecFactory,
      ImmutableSet<CQLWord> writeTimeVariables) {
    this.fieldsToVariables = fieldsToVariables;
    this.codecFactory = codecFactory;
    this.writeTimeVariables = writeTimeVariables;
    variablesToCodecs = Caffeine.newBuilder().build();
    variablesToFields = fieldsToVariables.inverse();
  }

  @NonNull
  @Override
  public Set<CQLWord> fieldToVariables(@NonNull Field field) {
    return fieldsToVariables.get(field);
  }

  @NonNull
  @Override
  public Set<Field> variableToFields(@NonNull CQLWord variable) {
    return variablesToFields.get(variable);
  }

  @NonNull
  @Override
  public Set<Field> fields() {
    return fieldsToVariables.keySet();
  }

  @NonNull
  @Override
  public Set<CQLWord> variables() {
    return variablesToFields.keySet();
  }

  @NonNull
  @Override
  public <T> TypeCodec<T> codec(
      @NonNull CQLWord variable,
      @NonNull DataType cqlType,
      @NonNull GenericType<? extends T> javaType) {
    @SuppressWarnings("unchecked")
    TypeCodec<T> codec =
        (TypeCodec<T>)
            variablesToCodecs.get(
                variable,
                n -> {
                  if (writeTimeVariables.contains(variable)) {
                    return createWritetimeCodec(cqlType, javaType);
                  } else return codecFactory.createConvertingCodec(cqlType, javaType, true);
                });
    assert codec != null;
    return codec;
  }

  @NonNull
  private <T> WriteTimeCodec<T> createWritetimeCodec(
      @NonNull DataType cqlType, @NonNull GenericType<T> javaType) {
    if (!cqlType.equals(DataTypes.BIGINT)) {
      throw new IllegalArgumentException("Cannot create a WriteTimeCodec for " + cqlType);
    }
    ConvertingCodec<T, Instant> innerCodec =
        codecFactory.createConvertingCodec(DataTypes.TIMESTAMP, javaType, true);
    return new WriteTimeCodec<>(innerCodec);
  }
}
