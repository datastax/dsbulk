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
import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.VARIABLE;
import static java.util.stream.Collectors.toSet;

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

  private final ImmutableMultimap<Field, CQLFragment> fieldsToVariables;
  private final ImmutableMultimap<CQLFragment, Field> variablesToFields;
  private final ExtendedCodecRegistry codecRegistry;
  private final Cache<MappingToken, TypeCodec<?>> variablesToCodecs;
  private final ImmutableSet<String> writeTimeVariables;

  public DefaultMapping(
      ImmutableMultimap<Field, CQLFragment> fieldsToVariables,
      ExtendedCodecRegistry codecRegistry,
      ImmutableSet<CQLFragment> writeTimeVariables) {
    this.fieldsToVariables = fieldsToVariables;
    this.codecRegistry = codecRegistry;
    this.writeTimeVariables =
        ImmutableSet.copyOf(
            writeTimeVariables.stream().map(var -> var.render(VARIABLE)).collect(toSet()));
    variablesToCodecs = Caffeine.newBuilder().build();
    variablesToFields = fieldsToVariables.inverse();
  }

  @NotNull
  @Override
  public Collection<CQLFragment> fieldToVariables(@NotNull Field field) {
    return fieldsToVariables.get(field);
  }

  @NotNull
  @Override
  public Collection<Field> variableToFields(@NotNull CQLFragment variable) {
    return variablesToFields.get(variable);
  }

  @NotNull
  @Override
  public Set<Field> fields() {
    return fieldsToVariables.keySet();
  }

  @NotNull
  @Override
  public Set<CQLFragment> variables() {
    return variablesToFields.keySet();
  }

  @NotNull
  @Override
  public <T> TypeCodec<T> codec(
      @NotNull CQLFragment variable,
      @NotNull DataType cqlType,
      @NotNull TypeToken<? extends T> javaType) {
    @SuppressWarnings("unchecked")
    TypeCodec<T> codec =
        (TypeCodec<T>)
            variablesToCodecs.get(
                variable,
                n -> {
                  // comparing by CQL "variable" form makes it possible for
                  // a resultset variable like "writetime(My Value)" to match
                  // a function call writetime("My Value"), since both have the
                  // same stringified variable form.
                  if (writeTimeVariables.contains(variable.render(VARIABLE))) {
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
