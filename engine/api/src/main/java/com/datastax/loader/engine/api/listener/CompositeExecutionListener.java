/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.api.listener;

import com.datastax.driver.core.Statement;
import com.datastax.loader.engine.api.exception.BulkExecutionException;
import com.datastax.loader.engine.api.result.Result;
import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;

/**
 * A composite {@link ExecutionListener} that forwards all received calls to its child listeners.
 */
public class CompositeExecutionListener implements ExecutionListener {

  private final List<ExecutionListener> listeners;

  public CompositeExecutionListener(ExecutionListener... listeners) {
    this(Arrays.asList(listeners));
  }

  public CompositeExecutionListener(Iterable<ExecutionListener> listeners) {
    this.listeners = ImmutableList.copyOf(listeners);
  }

  @Override
  public void onExecutionStarted(Statement statement, ExecutionContext context) {
    listeners.forEach(l -> l.onExecutionStarted(statement, context));
  }

  @Override
  public void onResultReceived(Result result, ExecutionContext context) {
    listeners.forEach(l -> l.onResultReceived(result, context));
  }

  @Override
  public void onExecutionCompleted(Statement statement, ExecutionContext context) {
    listeners.forEach(l -> l.onExecutionCompleted(statement, context));
  }

  @Override
  public void onExecutionFailed(BulkExecutionException exception, ExecutionContext context) {
    listeners.forEach(l -> l.onExecutionFailed(exception, context));
  }
}
