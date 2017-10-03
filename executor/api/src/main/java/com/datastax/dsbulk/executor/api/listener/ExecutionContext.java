/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
}
