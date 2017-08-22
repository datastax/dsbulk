/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.schema;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.google.common.reflect.TypeToken;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * Defines a bidirectional, one-to-one relationship between record fields and CQL columns.
 *
 * <p>In write workflows, CQL columns correspond to bound variables in the write statement. In read
 * workflows, CQL columns correspond to row variables in a read result.
 */
public interface Mapping {

  /**
   * Maps the given field to a bound statement variable. Used in write workflows.
   *
   * @param field the field name.
   * @return the bound statement variable name the given field maps to, or {@code null} if the field
   *     does not map to any known bound statement variable.
   */
  @Nullable
  String fieldToVariable(@NotNull String field);

  /**
   * Maps the given row variable to a field. Used in read workflows.
   *
   * @param variable the row variable name. The name must be {@link
   *     com.datastax.driver.core.Metadata#quoteIfNecessary(String) quoted if necessary}.
   * @return the field name the given variable maps to, or {@code null} if the variable does not map
   *     to any known field.
   */
  @Nullable
  String variableToField(@NotNull String variable);

  /**
   * Returns the codec to use for the given bound statement or row variable.
   *
   * @param variable the bound statement or row variable name; never {@code null}.
   * @param cqlType the CQL type; never {@code null}.
   * @param javaType the Java type; never {@code null}.
   * @return the codec to use; never {@code null}.
   * @throws CodecNotFoundException if a suitable codec cannot be found.
   */
  @NotNull
  TypeCodec<Object> codec(
      @NotNull String variable, @NotNull DataType cqlType, @NotNull TypeToken<?> javaType)
      throws CodecNotFoundException;
}
