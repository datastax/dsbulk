/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.executor.api.result;

import com.datastax.oss.driver.api.core.cql.Row;
import java.util.stream.Stream;

/** Represents the unique result of a write operation. */
public interface WriteResult extends Result {

  /**
   * If the query that produced this result was a conditional update, returns whether the
   * conditional update could be successfully applied.
   *
   * <p>If the query was unsuccessful (i.e., {@link #getError()} is not empty), then this method
   * always returns {@code false}; if the query was successful but wasn't a conditional update, then
   * this method always returns {@code true}.
   *
   * @return {@code true} if the conditional update was successfully applied, or if the write wasn't
   *     a conditional update; {@code false} otherwise.
   */
  boolean wasApplied();

  /**
   * If the query that produced this result was a conditional update, but the conditional update
   * wasn't successful, returns a {@link Stream} containing the failed writes.
   *
   * <p>If the query wasn't a conditional update, or if the conditional update was successful, this
   * method returns an empty stream.
   *
   * <p>Note that the returned stream will contain one single item for single inserts and updates;
   * only batch inserts and updates can generate a stream of more than one item.
   *
   * <p>The stream can only be consumed once.
   *
   * @return a {@link Stream} containing the failed writes, or an empty stream if no writes failed.
   */
  Stream<? extends Row> getFailedWrites();
}
