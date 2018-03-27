/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.connectors.api;

import java.net.URI;
import java.util.Collection;
import java.util.Set;
import org.jetbrains.annotations.NotNull;

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
   * Returns the record's position inside its {@linkplain #getResource() resource}, typically the
   * line number if the resource is a file, or the row number, if the resource is a database table.
   *
   * <p>Note that in the case of a file, the record's position is not necessarily equal to the line
   * number where it appears. First, if there is one or more header lines, the actual line number
   * would be its position + the number of header lines. Moreover, it the resource contains
   * multi-line records, then it is not possible to draw a direct relationship between the record's
   * position and its line.
   *
   * <p>Positions should be 1-based, i.e., the first record in the resource has position 1, and so
   * on. If the position cannot be determined, this method should return -1.
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
   * Usually, the record's location URI is derived from its {@linkplain #getResource() resource URI}
   * and its {@linkplain #getPosition() position}, but that is not a hard requirement.
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
  @NotNull
  Collection<Object> values();

  /**
   * Returns the value associated with the given field.
   *
   * <p>Note that a return value of {@code null} may indicate that the record contains no such
   * field, or that the field value was {@code null}.
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
