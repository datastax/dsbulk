/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.connectors.api;

import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.URI;
import java.util.Collection;
import java.util.Set;

/**
 * An item emitted by a {@link Connector}. A record is composed of one or more {@linkplain Field
 * fields}.
 *
 * <p>Records typically originate from a line in a file, or a row in a database table.
 */
public interface Record {

  /**
   * Returns the record source, typically a line in a file or a row in a database table.
   *
   * @return The record source.
   */
  @NonNull
  Object getSource();

  /**
   * Returns the record's resource location, typically the file or database table where it was
   * extracted from.
   *
   * <p>Details about the returned URI, and in particular its scheme, are implementation-specific.
   *
   * @return The record's resource.
   */
  @NonNull
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
   * Returns a set containing all the fields in this record.
   *
   * @return a set containing all the fields in this record.
   */
  @NonNull
  Set<Field> fields();

  /**
   * Returns a collection containing all the values in this record.
   *
   * <p>The iteration order of this collection should match that of {@link #fields()}.
   *
   * @return a collection containing all the values in this record.
   */
  @NonNull
  Collection<Object> values();

  /**
   * Returns the value associated with the given field.
   *
   * <p>Note that a return value of {@code null} may indicate that the record contains no such
   * field, or that the field value was {@code null}.
   *
   * @param field the field.
   * @return the value associated with the given field.
   */
  @Nullable
  Object getFieldValue(@NonNull Field field);

  /**
   * Clear all fields in this record.
   *
   * <p>This method should be used to free memory, when this record's fields have been successfully
   * processed and are no longer required.
   */
  void clear();
}
