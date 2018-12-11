/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import org.jetbrains.annotations.NotNull;

/**
 * A fragment of CQL language included in a mapping definition or in a CQL query. Fragments can be
 * {@linkplain CQLIdentifier CQL identifiers}, {@linkplain FunctionCall function calls}, or
 * {@linkplain CQLLiteral CQL literals}. In a mapping definition, they appear on the right side of a
 * mapping entry.
 */
public interface CQLFragment extends MappingToken {

  /**
   * @return The fragment's CQL form; for identifiers, this should be properly quoted if required.
   */
  @NotNull
  String asCql();

  /**
   * @return The fragment's name as a bound variable; this is usually the same as the CQL form,
   *     except for function calls where the variable name is a CQL identifier whose internal form
   *     is the internal form of the entire function call.
   */
  @NotNull
  default String asVariable() {
    return asCql();
  }

  /**
   * @return The fragment's internal form; for identifiers, this is the form stored in the driver's
   *     schema metadata and in the system tables.
   */
  @NotNull
  String asInternal();
}
