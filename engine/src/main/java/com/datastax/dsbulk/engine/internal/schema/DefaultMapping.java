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
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.codecs.writetime.WriteTimeCodec;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.reflect.TypeToken;
import java.time.Instant;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

public class DefaultMapping implements Mapping {

  private final ImmutableBiMap<String, String> fieldsToVariables;
  private final ExtendedCodecRegistry codecRegistry;
  private final Cache<String, TypeCodec<?>> variablesToCodecs;
  private final String writeTimeVariable;

  public DefaultMapping(
      ImmutableBiMap<String, String> fieldsToVariables,
      ExtendedCodecRegistry codecRegistry,
      String writeTimeVariable) {
    this.fieldsToVariables = fieldsToVariables;
    this.codecRegistry = codecRegistry;
    this.writeTimeVariable = writeTimeVariable;
    variablesToCodecs = Caffeine.newBuilder().build();
  }

  @Override
  public String fieldToVariable(String field) {
    return fieldsToVariables.get(field);
  }

  @Override
  public String variableToField(@NotNull String variable) {
    return fieldsToVariables.inverse().get(variable);
  }

  @Override
  public Set<String> fields() {
    return fieldsToVariables.keySet();
  }

  @Override
  public Set<String> variables() {
    return fieldsToVariables.values();
  }

  @Override
  public <T> TypeCodec<T> codec(
      @NotNull String variable, DataType cqlType, TypeToken<? extends T> javaType) {
    @SuppressWarnings("unchecked")
    TypeCodec<T> codec =
        (TypeCodec<T>)
            variablesToCodecs.get(
                variable,
                n -> {
                  if (variable.equals(writeTimeVariable)) {
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
