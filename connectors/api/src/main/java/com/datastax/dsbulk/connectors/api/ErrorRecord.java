/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.connectors.api;

/**
 * A {@link Record} that could not be properly processed.
 *
 * <p>It can be emitted in various situations, in particular:
 *
 * <ul>
 *   <li>When a {@link Connector} fails to create a record from data read from its external
 *       datasource (but subsequent reads are likely to succeed);
 *   <li>When a {@link Connector} fails to write a record to its external datasource (but subsequent
 *       writes are likely to succeed);
 *   <li>When a database {@link com.datastax.driver.core.Row row} cannot be converted into a record.
 * </ul>
 */
public interface ErrorRecord extends Record {

  /**
   * Returns the error that prevented this record from being processed.
   *
   * @return the error that prevented this record from being processed.
   */
  Throwable getError();
}
