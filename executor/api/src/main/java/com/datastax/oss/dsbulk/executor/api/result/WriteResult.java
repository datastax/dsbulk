/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.executor.api.result;

import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.Row;
import java.util.stream.Stream;

/** Represents the unique result of a write operation. */
public interface WriteResult extends Result {

  /**
   * Returns the size of the batch, if the executed statement was a {@link BatchStatement batch
   * statement}, or 1 otherwise.
   *
   * @return the size of the batch, or 1 if the statement was not a batch statement.
   */
  default int getBatchSize() {
    return getStatement() instanceof BatchStatement ? ((BatchStatement) getStatement()).size() : 1;
  }

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
