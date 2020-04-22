/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.workflow.commons.statement;

/**
 * A statement that has been produced in a bulk operation, and that keeps track of its original data
 * source.
 */
public interface BulkStatement<T> {

  /**
   * Returns the source of this statement.
   *
   * @return the source of this statement.
   */
  T getSource();
}
