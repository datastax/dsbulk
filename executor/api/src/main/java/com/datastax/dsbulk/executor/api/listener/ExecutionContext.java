/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.listener;

import java.util.Optional;

/**
 * The context of a statement execution.
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
