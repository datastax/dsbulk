/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api.result;

import com.datastax.driver.core.ResultSet;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Represents the unique result of a write operation.
 *
 * <p>Each result encapsulates the {@link ResultSet} returned by the execution of a {@link
 * #getStatement() write statement}.
 */
public interface WriteResult extends Result {

  /**
   * Returns the encapsulated {@link ResultSet} object for this write result, if present.
   *
   * <p>The value is present if the execution succeeded, and absent otherwise.
   *
   * @return the encapsulated {@link ResultSet} object for this write result, if present.
   */
  Optional<ResultSet> getResultSet();

  /**
   * If the result is a success, invoke the specified consumer with the returned {@link ResultSet},
   * otherwise do nothing.
   *
   * @param consumer block to be executed if the result is a success
   * @throws NullPointerException if the result is a success and {@code consumer} is null
   */
  default void ifSuccess(Consumer<? super ResultSet> consumer) {
    getResultSet().ifPresent(consumer);
  }
}
