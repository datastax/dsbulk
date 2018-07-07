/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.commons.codecs.writetime.WriteTimeCodec;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableBiMap;
import java.time.Instant;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

public class DefaultMapping implements Mapping {

  private final ImmutableBiMap<String, CqlIdentifier> fieldsToVariables;
  private final ExtendedCodecRegistry codecRegistry;
  private final Cache<CqlIdentifier, TypeCodec<?>> variablesToCodecs;
  private final CqlIdentifier writeTimeVariable;

  public DefaultMapping(
      ImmutableBiMap<String, CqlIdentifier> fieldsToVariables,
      ExtendedCodecRegistry codecRegistry,
      CqlIdentifier writeTimeVariable) {
    this.fieldsToVariables = fieldsToVariables;
    this.codecRegistry = codecRegistry;
    this.writeTimeVariable = writeTimeVariable;
    variablesToCodecs = Caffeine.newBuilder().build();
  }

  @Override
  public CqlIdentifier fieldToVariable(@NotNull String field) {
    return fieldsToVariables.get(field);
  }

  @Override
  public String variableToField(@NotNull CqlIdentifier variable) {
    return fieldsToVariables.inverse().get(variable);
  }

  @Override
  public Set<String> fields() {
    return fieldsToVariables.keySet();
  }

  @Override
  public Set<CqlIdentifier> variables() {
    return fieldsToVariables.values();
  }

  @NotNull
  @Override
  public <T> TypeCodec<T> codec(
      @NotNull CqlIdentifier variable,
      @NotNull DataType cqlType,
      @NotNull GenericType<? extends T> javaType) {
    @SuppressWarnings("unchecked")
    TypeCodec<T> codec =
        (TypeCodec<T>)
            variablesToCodecs.get(
                variable,
                n -> {
                  if (variable.equals(writeTimeVariable)) {
                    if (!cqlType.equals(DataTypes.BIGINT)) {
                      throw new IllegalArgumentException(
                          "Cannot create a WriteTimeCodec for " + cqlType);
                    }
                    ConvertingCodec<T, Instant> innerCodec =
                        codecRegistry.convertingCodecFor(DataTypes.TIMESTAMP, javaType);
                    return new WriteTimeCodec<>(innerCodec);
                  }
                  return codecRegistry.codecFor(cqlType, javaType);
                });
    assert codec != null;
    return codec;
  }
}
