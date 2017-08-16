/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.schema;

import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.TypeCodec;
import com.datastax.loader.engine.internal.codecs.ExtendedCodecRegistry;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.ImmutableMap;
import org.jetbrains.annotations.NotNull;

public class DefaultMapping implements Mapping {

  private final ImmutableMap<Object, String> fieldsToVariables;
  private final ExtendedCodecRegistry codecRegistry;
  private final ColumnDefinitions variables;
  private final Cache<String, TypeCodec<Object>> variablesToCodecs;

  public DefaultMapping(
      ImmutableMap<Object, String> fieldsToVariables,
      ExtendedCodecRegistry codecRegistry,
      ColumnDefinitions variables) {
    this.fieldsToVariables = fieldsToVariables;
    this.codecRegistry = codecRegistry;
    this.variables = variables;
    variablesToCodecs = Caffeine.newBuilder().build();
  }

  @Override
  public String map(@NotNull Object field) {
    return fieldsToVariables.get(field);
  }

  @NotNull
  @Override
  public TypeCodec<Object> codec(@NotNull String name, @NotNull Object raw) {
    TypeCodec<Object> codec =
        variablesToCodecs.get(
            name, n -> codecRegistry.codecFor(variables.getType(n), raw.getClass()));
    assert codec != null;
    return codec;
  }
}
