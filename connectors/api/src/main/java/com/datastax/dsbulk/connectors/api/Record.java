/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.api;

import java.net.URI;
import java.util.Collection;
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
   * Returns the record's resource location, typically the file or database table where it was
   * extracted from.
   *
   * <p>Details about the returned URI, and in particular its scheme, are implementation-specific.
   *
   * @return The record's resource.
   */
  URI getResource();

  /**
   * Returns the record's position inside its {@link #getResource() resource}, typically the line
   * number if the resource is a file, or the row number, if the resource is a database table.
   *
   * <p>Positions should be 1-based. If the position cannot be determined, this method should return
   * -1.
   *
   * @return the record's position, or -1 if the position cannot be determined.
   */
  long getPosition();

  /**
   * Returns the record location, typically its position in a file or its location in the origin
   * database table.
   *
   * <p>Locations are mostly useful to help diagnose errors, for example to locate a record that
   * could not be written to the database.
   *
   * <p>Details about the returned URI, and in particular its scheme, are implementation-specific.
   * Usually, the record's location URI is derived from its {@link #getResource() resource URI} and
   * its {@link #getPosition() position}, but that is not a hard requirement.
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
   * Returns a collection containing all the values in this record.
   *
   * <p>The iteration order of this collection should match that of {@link #fields()}.
   *
   * @return a collection containing all the values in this record.
   */
  Collection<Object> values();

  /**
   * Returns the value associated with the given field.
   *
   * @param field the field name.
   * @return the value associated with the given field.
   */
  Object getFieldValue(String field);

  /**
   * Clear all fields in this record.
   *
   * <p>This method should be used to free memory, when this record's fields have been successfully
   * processed and are no longer required.
   */
  void clear();
}
