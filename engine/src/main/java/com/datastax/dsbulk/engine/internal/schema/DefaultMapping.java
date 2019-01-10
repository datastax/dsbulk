/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.driver.core.DataType.Name.BIGINT;
import static com.datastax.driver.core.DataType.timestamp;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.dsbulk.connectors.api.Field;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.codecs.writetime.WriteTimeCodec;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import java.time.Instant;
import java.util.Collection;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

public class DefaultMapping implements Mapping {

  private final ImmutableMultimap<Field, CQLIdentifier> fieldsToVariables;
  private final ImmutableMultimap<CQLIdentifier, Field> variablesToFields;
  private final ExtendedCodecRegistry codecRegistry;
  private final Cache<MappingToken, TypeCodec<?>> variablesToCodecs;
  private final ImmutableSet<CQLIdentifier> writeTimeVariables;

  public DefaultMapping(
      ImmutableMultimap<Field, CQLIdentifier> fieldsToVariables,
      ExtendedCodecRegistry codecRegistry,
      ImmutableSet<CQLIdentifier> writeTimeVariables) {
    this.fieldsToVariables = fieldsToVariables;
    this.codecRegistry = codecRegistry;
    this.writeTimeVariables = writeTimeVariables;
    variablesToCodecs = Caffeine.newBuilder().build();
    variablesToFields = fieldsToVariables.inverse();
  }

  @NotNull
  @Override
  public Collection<CQLIdentifier> fieldToVariables(@NotNull Field field) {
    return fieldsToVariables.get(field);
  }

  @NotNull
  @Override
  public Collection<Field> variableToFields(@NotNull CQLIdentifier variable) {
    return variablesToFields.get(variable);
  }

  @NotNull
  @Override
  public Set<Field> fields() {
    return fieldsToVariables.keySet();
  }

  @NotNull
  @Override
  public Set<CQLIdentifier> variables() {
    return variablesToFields.keySet();
  }

  @NotNull
  @Override
  public <T> TypeCodec<T> codec(
      @NotNull CQLIdentifier variable,
      @NotNull DataType cqlType,
      @NotNull TypeToken<? extends T> javaType) {
    @SuppressWarnings("unchecked")
    TypeCodec<T> codec =
        (TypeCodec<T>)
            variablesToCodecs.get(
                variable,
                n -> {
                  if (writeTimeVariables.contains(variable)) {
                    if (cqlType.getName() != BIGINT) {
                      throw new IllegalArgumentException(
                          "Cannot create a WriteTimeCodec for " + cqlType);
                    }
                    ConvertingCodec<T, Instant> innerCodec =
                        codecRegistry.convertingCodecFor(timestamp(), javaType);
                    return new WriteTimeCodec<>(innerCodec);
                  }
                  return codecRegistry.codecFor(cqlType, javaType);
                });
    assert codec != null;
    return codec;
  }
}
