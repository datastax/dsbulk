/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.schema;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.loader.engine.internal.codecs.ExtendedCodecRegistry;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.reflect.TypeToken;
import org.jetbrains.annotations.NotNull;

public class DefaultMapping implements Mapping {

  private final ImmutableBiMap<String, String> fieldsToVariables;
  private final ExtendedCodecRegistry codecRegistry;
  private final Cache<String, TypeCodec<Object>> variablesToCodecs;

  public DefaultMapping(
      ImmutableBiMap<String, String> fieldsToVariables, ExtendedCodecRegistry codecRegistry) {
    this.fieldsToVariables = fieldsToVariables;
    this.codecRegistry = codecRegistry;
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
  public TypeCodec<Object> codec(
      @NotNull String variable, DataType cqlType, TypeToken<?> javaType) {
    TypeCodec<Object> codec =
        variablesToCodecs.get(variable, n -> codecRegistry.codecFor(cqlType, javaType));
    assert codec != null;
    return codec;
  }
}
