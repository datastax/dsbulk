/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.mapping;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSet;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableSetMultimap;
import com.datastax.oss.dsbulk.codecs.ConvertingCodec;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.writetime.WriteTimeCodec;
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
