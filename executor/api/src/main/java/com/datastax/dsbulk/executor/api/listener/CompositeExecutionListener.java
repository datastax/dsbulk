/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.listener;

import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
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
