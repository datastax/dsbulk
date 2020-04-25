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
package com.datastax.oss.dsbulk.executor.api.listener;

import com.datastax.oss.driver.api.core.cql.Statement;
import java.util.Optional;

/**
 * The context of a statement or request execution.
 *
 * <p>The scope of a context can be either global, if it is associated with a global execution event
 * such as {@link ExecutionListener#onExecutionStarted(Statement, ExecutionContext)
 * onExecutionStarted} or {@link ExecutionListener#onExecutionSuccessful(Statement,
 * ExecutionContext) onExecutionSuccessful}; or local to a request-response cycle, if it is
 * associated with a request execution event such as {@link
 * ExecutionListener#onWriteRequestStarted(Statement, ExecutionContext)} or {@link
 * ExecutionListener#onWriteRequestSuccessful(Statement, ExecutionContext)}.
 *
 * <p>See the javadocs of {@link ExecutionListener} to understand when its scope is global or local.
 *
 * @see ExecutionListener
 */
public interface ExecutionContext {

  /**
   * Sets an attribute into this context.
   *
   * @param key the attribute key. Cannot be {@code null}.
   * @param value the attribute value. Can be {@code null}.
   */
  void setAttribute(Object key, Object value);

  /**
   * Retrieves an attribute stored in this context.
   *
   * @param key the attribute key. Cannot be {@code null}.
   * @return an {@link Optional} containing the attribute value, if present.
   */
  Optional<Object> getAttribute(Object key);

  /**
   * Returns the elapsed time, in nanoseconds, between start and end of execution.
   *
   * <p>The exact meaning of the value returned by this method depends on whether the context is
   * global or local: for global contexts, this is the elapsed time for the whole statement
   * execution; for local ones, this is the elapsed time for a single request-response cycle.
   *
   * @return the elapsed time, in nanoseconds, between start and end of execution; or -1, if the
   *     execution hasn't finished yet.
   */
  long elapsedTimeNanos();
}
