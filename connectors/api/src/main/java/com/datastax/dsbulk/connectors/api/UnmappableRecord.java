/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.api;

/**
 * A {@link Record} that could not be properly parsed, either from its external source, if this
 * record was read by a {@link Connector}, or from its corresponding {@link
 * com.datastax.driver.core.Row row}, if this record was being read from the database.
 */
public interface UnmappableRecord extends Record {

  /**
   * Returns the error that prevented this record from being parsed.
   *
   * @return the error that prevented this record from being parsed.
   */
  Throwable getError();
}
