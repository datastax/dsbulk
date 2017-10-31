/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.listener;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.internal.histogram.HdrHistogramReservoir;

/** A {@link ExecutionListener} that records useful metrics about the ongoing bulk operations. */
public class MetricsCollectingExecutionListener implements ExecutionListener {

  private final MetricRegistry registry;

  private final Timer statementsTimer;
  private final Counter successfulStatementsCounter;
  private final Counter failedStatementsCounter;

  private final Timer readsTimer;
  private final Counter successfulReadsCounter;
  private final Counter failedReadsCounter;

  private final Timer writesTimer;
  private final Counter successfulWritesCounter;
  private final Counter failedWritesCounter;

  private final Timer readsWritesTimer;
  private final Counter successfulReadsWritesCounter;
  private final Counter failedReadsWritesCounter;

  private final Counter inFlightRequestsCounter;

  /** Creates a new instance using a newly-allocated {@link MetricRegistry}. */
  public MetricsCollectingExecutionListener() {
    this(new MetricRegistry());
  }

  /**
   * Creates a new instance using the given {@link MetricRegistry}.
   *
   * @param registry The {@link MetricRegistry} to use.
   */
  public MetricsCollectingExecutionListener(MetricRegistry registry) {
    this.registry = registry;

    statementsTimer =
        registry.timer("executor/statements/total", () -> new Timer(new HdrHistogramReservoir()));
    successfulStatementsCounter = registry.counter("executor/statements/successful");
    failedStatementsCounter = registry.counter("executor/statements/failed");

    readsTimer =
        registry.timer("executor/reads/total", () -> new Timer(new HdrHistogramReservoir()));
    successfulReadsCounter = registry.counter("executor/reads/successful");
    failedReadsCounter = registry.counter("executor/reads/failed");

    writesTimer =
        registry.timer("executor/writes/total", () -> new Timer(new HdrHistogramReservoir()));
    successfulWritesCounter = registry.counter("executor/writes/successful");
    failedWritesCounter = registry.counter("executor/writes/failed");

    readsWritesTimer =
        registry.timer("executor/reads-writes/total", () -> new Timer(new HdrHistogramReservoir()));
    successfulReadsWritesCounter = registry.counter("executor/reads-writes/successful");
    failedReadsWritesCounter = registry.counter("executor/reads-writes/failed");

    inFlightRequestsCounter = registry.counter("executor/in-flight");
  }

  /**
   * Returns the {@link MetricRegistry} used to aggregate metrics for this listener.
   *
   * @return the {@link MetricRegistry} used to aggregate metrics for this listener.
   */
  public MetricRegistry getRegistry() {
    return registry;
  }

  /**
   * Returns a {@link Timer} for total statement executions (successful and failed).
   *
   * <p>A batch statement is counted as one single statement. If that's not what you want, you are
   * probably looking for {@link #getWritesTimer()}.
   *
   * @return a {@link Timer} for total statement executions (successful and failed).
   */
  public Timer getStatementsTimer() {
    return statementsTimer;
  }

  /**
   * Returns a {@link Counter} for successful statement executions.
   *
   * <p>A batch statement is counted as one single statement. If that's not what you want, you are
   * probably looking for {@link #getSuccessfulWritesCounter()}.
   *
   * @return a {@link Counter} for successful statement executions.
   */
  public Counter getSuccessfulStatementsCounter() {
    return successfulStatementsCounter;
  }

  /**
   * Returns a {@link Counter} for failed statement executions.
   *
   * <p>A batch statement is counted as one single statement. If that's not what you want, you are
   * probably looking for {@link #getFailedWritesCounter()}.
   *
   * @return a {@link Counter} for failed statement executions.
   */
  public Counter getFailedStatementsCounter() {
    return failedStatementsCounter;
  }

  /**
   * Returns a {@link Timer} that evaluates the duration of execution of reads, both successful and
   * failed.
   *
   * @return a {@link Timer} that evaluates the duration of execution of reads, both successful and
   *     failed.
   */
  public Timer getReadsTimer() {
    return readsTimer;
  }

  /**
   * Returns a {@link Counter} that evaluates the duration of execution of successful reads.
   *
   * @return a {@link Counter} that evaluates the duration of execution of successful reads.
   */
  public Counter getSuccessfulReadsCounter() {
    return successfulReadsCounter;
  }

  /**
   * Returns a {@link Counter} that evaluates the duration of execution of failed reads.
   *
   * @return a {@link Counter} that evaluates the duration of execution of failed reads.
   */
  public Counter getFailedReadsCounter() {
    return failedReadsCounter;
  }

  /**
   * Returns a {@link Timer} that evaluates the duration of execution of writes, both successful and
   * failed.
   *
   * <p>A batch statement is counted as many times as the number of child statements it contains. If
   * that's not what you want, you are probably looking for {@link #getStatementsTimer()}.
   *
   * @return a {@link Timer} that evaluates the duration of execution of writes, both successful and
   *     failed.
   */
  public Timer getWritesTimer() {
    return writesTimer;
  }

  /**
   * Returns a {@link Counter} that evaluates the duration of execution of successful writes.
   *
   * <p>A batch statement is counted as many times as the number of child statements it contains. If
   * that's not what you want, you are probably looking for {@link
   * #getSuccessfulStatementsCounter()}.
   *
   * @return a {@link Counter} that evaluates the duration of execution of successful writes.
   */
  public Counter getSuccessfulWritesCounter() {
    return successfulWritesCounter;
  }

  /**
   * Returns a {@link Counter} that evaluates the duration of execution of failed writes.
   *
   * <p>A batch statement is counted as many times as the number of child statements it contains. If
   * that's not what you want, you are probably looking for {@link #getFailedStatementsCounter()}.
   *
   * @return a {@link Counter} that evaluates the duration of execution of failed writes.
   */
  public Counter getFailedWritesCounter() {
    return failedWritesCounter;
  }

  /**
   * Returns a {@link Timer} that evaluates the duration of execution of all operations, including
   * reads and writes, both successful and failed.
   *
   * @return a {@link Timer} that evaluates the duration of execution of all operations.
   */
  public Timer getReadsWritesTimer() {
    return readsWritesTimer;
  }

  /**
   * Returns a {@link Counter} that evaluates the duration of execution of all successful
   * operations, including reads and writes.
   *
   * @return a {@link Counter} that evaluates the duration of execution of all successful
   *     operations.
   */
  public Counter getSuccessfulReadsWritesCounter() {
    return successfulReadsWritesCounter;
  }

  /**
   * Returns a {@link Counter} that evaluates the duration of execution of all failed operations,
   * including reads and writes.
   *
   * @return a {@link Counter} that evaluates the duration of execution of all failed operations.
   */
  public Counter getFailedReadsWritesCounter() {
    return failedReadsWritesCounter;
  }

  /**
   * Returns a {@link Counter} that evaluates the number of current in-flight requests, i.e. the
   * number of uncompleted futures waiting for a response from the server.
   *
   * @return a {@link Counter} that evaluates the number of current in-flight requests.
   */
  public Counter getInFlightRequestsCounter() {
    return inFlightRequestsCounter;
  }

  @Override
  public void onWriteRequestStarted(Statement statement, ExecutionContext context) {
    inFlightRequestsCounter.inc();
  }

  @Override
  public void onReadRequestStarted(Statement statement, ExecutionContext context) {
    inFlightRequestsCounter.inc();
  }

  @Override
  public void onWriteRequestSuccessful(Statement statement, ExecutionContext context) {
    int delta = delta(statement);
    stop(context, writesTimer, delta);
    stop(context, readsWritesTimer, delta);
    successfulWritesCounter.inc(delta);
    successfulReadsWritesCounter.inc(delta);
    inFlightRequestsCounter.dec();
  }

  @Override
  public void onWriteRequestFailed(Statement statement, Throwable error, ExecutionContext context) {
    int delta = delta(statement);
    stop(context, writesTimer, delta);
    stop(context, readsWritesTimer, delta);
    failedWritesCounter.inc(delta);
    failedReadsWritesCounter.inc(delta);
    inFlightRequestsCounter.dec();
  }

  @Override
  public void onReadRequestSuccessful(Statement statement, ExecutionContext context) {
    inFlightRequestsCounter.dec();
  }

  @Override
  public void onRowReceived(Row row, ExecutionContext context) {
    stop(context, readsTimer, 1);
    stop(context, readsWritesTimer, 1);
    successfulReadsCounter.inc(1);
    successfulReadsWritesCounter.inc(1);
  }

  @Override
  public void onReadRequestFailed(Statement statement, Throwable error, ExecutionContext context) {
    stop(context, readsTimer, 1);
    stop(context, readsWritesTimer, 1);
    failedReadsCounter.inc();
    failedReadsWritesCounter.inc();
    inFlightRequestsCounter.dec();
  }

  @Override
  public void onExecutionSuccessful(Statement statement, ExecutionContext context) {
    stop(context, statementsTimer, 1);
    successfulStatementsCounter.inc();
  }

  @Override
  public void onExecutionFailed(BulkExecutionException exception, ExecutionContext context) {
    stop(context, statementsTimer, 1);
    failedStatementsCounter.inc();
  }

  private static void stop(ExecutionContext context, Timer timer, int delta) {
    long elapsed = context.elapsedTimeNanos();
    for (int i = 0; i < delta; i++) {
      timer.update(elapsed, NANOSECONDS);
    }
  }

  private static int delta(Statement statement) {
    if (statement instanceof BatchStatement) {
      return ((BatchStatement) statement).size();
    } else {
      return 1;
    }
  }
}
