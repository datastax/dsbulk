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

import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException;
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
  public void onExecutionStarted(Statement<?> statement, ExecutionContext context) {
    listeners.forEach(l -> l.onExecutionStarted(statement, context));
  }

  @Override
  public void onExecutionSuccessful(Statement<?> statement, ExecutionContext context) {
    listeners.forEach(l -> l.onExecutionSuccessful(statement, context));
  }

  @Override
  public void onExecutionFailed(BulkExecutionException exception, ExecutionContext context) {
    listeners.forEach(l -> l.onExecutionFailed(exception, context));
  }

  @Override
  public void onWriteRequestStarted(Statement<?> statement, ExecutionContext context) {
    listeners.forEach(l -> l.onWriteRequestStarted(statement, context));
  }

  @Override
  public void onWriteRequestSuccessful(Statement<?> statement, ExecutionContext context) {
    listeners.forEach(l -> l.onWriteRequestSuccessful(statement, context));
  }

  @Override
  public void onWriteRequestFailed(
      Statement<?> statement, Throwable error, ExecutionContext context) {
    listeners.forEach(l -> l.onWriteRequestFailed(statement, error, context));
  }

  @Override
  public void onReadRequestStarted(Statement<?> statement, ExecutionContext context) {
    listeners.forEach(l -> l.onReadRequestStarted(statement, context));
  }

  @Override
  public void onReadRequestSuccessful(Statement<?> statement, ExecutionContext context) {
    listeners.forEach(l -> l.onReadRequestSuccessful(statement, context));
  }

  @Override
  public void onReadRequestFailed(
      Statement<?> statement, Throwable error, ExecutionContext context) {
    listeners.forEach(l -> l.onReadRequestFailed(statement, error, context));
  }

  @Override
  public void onRowReceived(Row row, ExecutionContext context) {
    listeners.forEach(l -> l.onRowReceived(row, context));
  }
}
