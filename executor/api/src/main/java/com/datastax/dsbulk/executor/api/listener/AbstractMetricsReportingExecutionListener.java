/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.listener;

import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.ScheduledReporter;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * An {@link ExecutionListener} that reports useful metrics about ongoing bulk read and write
 * operations. It relies on a delegate {@link MetricsCollectingExecutionListener} as its source of
 * metrics.
 */
public abstract class AbstractMetricsReportingExecutionListener extends ScheduledReporter
    implements ExecutionListener {

  protected static final double BYTES_PER_KB = 1024;
  protected static final double BYTES_PER_MB = 1024 * 1024;

  private final MetricsCollectingExecutionListener delegate;

  protected AbstractMetricsReportingExecutionListener(
      MetricsCollectingExecutionListener delegate,
      String name,
      MetricFilter filter,
      TimeUnit rateUnit,
      TimeUnit durationUnit) {
    super(delegate.getRegistry(), name, filter, rateUnit, durationUnit);
    this.delegate = delegate;
  }

  protected AbstractMetricsReportingExecutionListener(
      MetricsCollectingExecutionListener delegate,
      String name,
      MetricFilter filter,
      TimeUnit rateUnit,
      TimeUnit durationUnit,
      ScheduledExecutorService scheduler) {
    super(delegate.getRegistry(), name, filter, rateUnit, durationUnit, scheduler);
    this.delegate = delegate;
  }

  @Override
  public void onExecutionStarted(Statement statement, ExecutionContext context) {
    delegate.onExecutionStarted(statement, context);
  }

  @Override
  public void onWriteRequestStarted(Statement statement, ExecutionContext context) {
    delegate.onWriteRequestStarted(statement, context);
  }

  @Override
  public void onReadRequestStarted(Statement statement, ExecutionContext context) {
    delegate.onReadRequestStarted(statement, context);
  }

  @Override
  public void onWriteRequestSuccessful(Statement statement, ExecutionContext context) {
    delegate.onWriteRequestSuccessful(statement, context);
  }

  @Override
  public void onWriteRequestFailed(Statement statement, Throwable error, ExecutionContext context) {
    delegate.onWriteRequestFailed(statement, error, context);
  }

  @Override
  public void onReadRequestSuccessful(Statement statement, ExecutionContext context) {
    delegate.onReadRequestSuccessful(statement, context);
  }

  @Override
  public void onRowReceived(Row row, ExecutionContext context) {
    delegate.onRowReceived(row, context);
  }

  @Override
  public void onReadRequestFailed(Statement statement, Throwable error, ExecutionContext context) {
    delegate.onReadRequestFailed(statement, error, context);
  }

  @Override
  public void onExecutionSuccessful(Statement statement, ExecutionContext context) {
    delegate.onExecutionSuccessful(statement, context);
  }

  @Override
  public void onExecutionFailed(BulkExecutionException exception, ExecutionContext context) {
    delegate.onExecutionFailed(exception, context);
  }
}
