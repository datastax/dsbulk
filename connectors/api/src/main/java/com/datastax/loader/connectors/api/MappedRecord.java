/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.connectors.api;

import java.util.Set;

/** */
public interface MappedRecord extends Record {

  /**
   * Returns a set containing all the field names and/or indexes in this record.
   *
   * <p>The returned set may contain both Strings representing field names and Integers representing
   * zero-based field indexes, depending on the connector capabilities to produce field names and/or
   * field indexes.
   *
   * @return a set containing all the field names and/or indexes in this record.
   */
  Set<Object> fields();

  /**
   * Returns the value associated with the given field.
   *
   * @param field either a String representing a field name or an Integer representing a zero-based
   *     field index.
   * @return the value associated with the given field.
   */
  Object getFieldValue(Object field);
}
