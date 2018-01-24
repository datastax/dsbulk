/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.result;

import com.datastax.driver.core.Row;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * Represents one of the many results of a read operation.
 *
 * <p>Each result encapsulates one {@link Row} returned by the execution of a {@link #getStatement()
 * read statement}.
 */
public interface ReadResult extends Result {

  /**
   * Returns the encapsulated {@link Row} object for this read result, if present.
   *
   * <p>The value is present if the execution succeeded, and absent otherwise.
   *
   * @return the encapsulated {@link Row} object for this read result, if present.
   */
  Optional<Row> getRow();

  /**
   * If the result is a success, invoke the specified consumer with the returned {@link Row},
   * otherwise do nothing.
   *
   * @param consumer block to be executed if the result is a success
   * @throws NullPointerException if the result is a success and {@code consumer} is null
   */
  default void ifSuccess(Consumer<? super Row> consumer) {
    getRow().ifPresent(consumer);
  }
}
