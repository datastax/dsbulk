/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.internal.listener;

import com.datastax.dsbulk.executor.api.listener.ExecutionContext;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** */
public class DefaultExecutionContext implements ExecutionContext {

  private final ConcurrentMap<Object, Object> attributes = new ConcurrentHashMap<>();

  @Override
  public void setAttribute(Object key, Object value) {
    attributes.put(key, value);
  }

  @Override
  public Optional<Object> getAttribute(Object key) {
    return Optional.ofNullable(attributes.get(key));
  }
}
