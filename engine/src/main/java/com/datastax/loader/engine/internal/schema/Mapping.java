/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.schema;

public interface Mapping {

  /**
   * Maps the given field to a bound variable.
   *
   * @param field either a String representing a field name, or an Integer representing a zero-based
   *     field index.
   * @return the bound variable name the given field maps to, or {@code null} if the field does not
   *     map to any known bound variable.
   */
  String map(Object field);
}
