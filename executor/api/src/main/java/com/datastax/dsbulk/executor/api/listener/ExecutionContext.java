/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.listener;

import com.datastax.driver.core.Statement;
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
