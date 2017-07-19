/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api.statement;

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
