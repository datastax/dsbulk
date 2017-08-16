/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.schema;

import com.datastax.driver.core.TypeCodec;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public interface Mapping {

  /**
   * Maps the given field to a bound variable.
   *
   * @param field either a String representing a field name, or an Integer representing a zero-based
   *     field index.
   * @return the bound variable name the given field maps to, or {@code null} if the field does not
   *     map to any known bound variable.
   */
  @Nullable
  String map(@NotNull Object field);

  /**
   * Returns the codec to use for the given bound variable.
   *
   * @param name the bound variable name; never {@code null}.
   * @param raw The raw value to find a codec for, as emitted by the connector; never {@code null}.
   * @return the codec to use; never {@code null}.
   */
  @NotNull
  TypeCodec<Object> codec(@NotNull String name, @NotNull Object raw);
}
