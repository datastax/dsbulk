/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.listener;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
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
  public void onExecutionSuccessful(Statement statement, ExecutionContext context) {
    listeners.forEach(l -> l.onExecutionSuccessful(statement, context));
  }

  @Override
  public void onExecutionFailed(BulkExecutionException exception, ExecutionContext context) {
    listeners.forEach(l -> l.onExecutionFailed(exception, context));
  }

  @Override
  public void onWriteRequestStarted(Statement statement, ExecutionContext context) {
    listeners.forEach(l -> l.onWriteRequestStarted(statement, context));
  }

  @Override
  public void onWriteRequestSuccessful(Statement statement, ExecutionContext context) {
    listeners.forEach(l -> l.onWriteRequestSuccessful(statement, context));
  }

  @Override
  public void onWriteRequestFailed(Statement statement, Throwable error, ExecutionContext context) {
    listeners.forEach(l -> l.onWriteRequestFailed(statement, error, context));
  }

  @Override
  public void onReadRequestStarted(Statement statement, ExecutionContext context) {
    listeners.forEach(l -> l.onReadRequestStarted(statement, context));
  }

  @Override
  public void onReadRequestSuccessful(Statement statement, ExecutionContext context) {
    listeners.forEach(l -> l.onReadRequestSuccessful(statement, context));
  }

  @Override
  public void onReadRequestFailed(Statement statement, Throwable error, ExecutionContext context) {
    listeners.forEach(l -> l.onReadRequestFailed(statement, error, context));
  }

  @Override
  public void onRowReceived(Row row, ExecutionContext context) {
    listeners.forEach(l -> l.onRowReceived(row, context));
  }
}
