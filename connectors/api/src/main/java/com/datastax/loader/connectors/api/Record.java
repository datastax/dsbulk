/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.connectors.api;

import java.net.URI;
import java.util.Set;

/**
 * An item emitted by a {@link Connector}.
 *
 * <p>Records typically originate from a line in a file, or a row in a database table.
 */
public interface Record {

  /**
   * Returns the record source, typically a line in a file or a row in a database table.
   *
   * @return The record source.
   */
  Object getSource();

  /**
   * Returns the record location, typically its position in a file or its location in the origin
   * database table.
   *
   * <p>Locations are mostly useful to help diagnose errors, for example to locate a record that
   * could not be written to the database.
   *
   * <p>Details about the returned URI, and in particular its scheme, are implementation-specific.
   *
   * @return The record location.
   */
  URI getLocation();

  /**
   * Returns a set containing all the field names in this record.
   *
   * @return a set containing all the field names in this record.
   */
  Set<String> fields();

  /**
   * Returns the value associated with the given field.
   *
   * @param field the field name.
   * @return the value associated with the given field.
   */
  Object getFieldValue(String field);
}
