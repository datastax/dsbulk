/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import com.datastax.driver.core.DataType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.CodecNotFoundException;
import com.datastax.dsbulk.connectors.api.Field;
import com.google.common.reflect.TypeToken;
import java.util.Collection;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

/**
 * Defines a bidirectional, one-to-one relationship between record fields and CQL columns.
 *
 * <p>In write workflows, CQL columns correspond to bound variables in the write statement. In read
 * workflows, CQL columns correspond to row variables in a read result.
 */
public interface Mapping {

  /**
   * Maps the given field to one or more bound statement variables.
   *
   * @param field the field to find the variable for.
   * @return the bound statement variables the given field maps to, or an empty collection if the
   *     field does not map to any known bound statement variable.
   */
  @NotNull
  Collection<CQLFragment> fieldToVariables(@NotNull Field field);

  /**
   * Maps the given row variable to a field.
   *
   * @param variable the row variable; never {@code null}.
   * @return the fields the given variable maps to, or an empty collection if the variable does not
   *     map to any known field.
   */
  @NotNull
  Collection<Field> variableToFields(@NotNull CQLFragment variable);

  /**
   * Returns the codec to use for the given bound statement or row variable.
   *
   * <p>Note that the given variable name must be supplied unquoted, even if it requires quoting to
   * comply with the syntax of CQL identifiers.
   *
   * @param <T> the codec's Java type.
   * @param variable the bound statement or row variable; never {@code null}.
   * @param cqlType the CQL type; never {@code null}.
   * @param javaType the Java type; never {@code null}.
   * @return the codec to use; never {@code null}.
   * @throws CodecNotFoundException if a suitable codec cannot be found.
   */
  @NotNull
  <T> TypeCodec<T> codec(
      @NotNull CQLFragment variable,
      @NotNull DataType cqlType,
      @NotNull TypeToken<? extends T> javaType)
      throws CodecNotFoundException;

  /**
   * Returns all the fields in this mapping.
   *
   * @return the fields in this mapping.
   */
  @NotNull
  Set<Field> fields();

  /**
   * Returns all the variables in this mapping.
   *
   * @return the variables in this mapping.
   */
  @NotNull
  Set<CQLFragment> variables();
}
