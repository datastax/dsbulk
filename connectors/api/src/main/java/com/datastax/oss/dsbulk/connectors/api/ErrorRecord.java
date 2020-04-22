/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.connectors.api;

import edu.umd.cs.findbugs.annotations.NonNull;

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
 *   <li>When a database {@linkplain com.datastax.oss.driver.api.core.cql.Row row} cannot be
 *       converted into a record.
 * </ul>
 */
public interface ErrorRecord extends Record {

  /**
   * Returns the error that prevented this record from being processed.
   *
   * @return the error that prevented this record from being processed.
   */
  @NonNull
  Throwable getError();
}
