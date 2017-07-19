/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api.listener;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Statement;
import com.datastax.loader.executor.api.exception.BulkExecutionException;
import com.datastax.loader.executor.api.result.Result;
import com.datastax.loader.executor.api.result.ReadResult;
import java.util.stream.IntStream;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

/** A {@link ExecutionListener} that records useful metrics about the ongoing bulk operations. */
public class MetricsCollectingExecutionListener implements ExecutionListener {

  private final MetricRegistry registry;

  private final Timer totalStatementsTimer;
  private final Timer successfulStatementsTimer;
  private final Timer failedStatementsTimer;

  private final Timer totalReadsTimer;
  private final Timer successfulReadsTimer;
  private final Timer failedReadsTimer;

  private final Timer totalWritesTimer;
  private final Timer successfulWritesTimer;
  private final Timer failedWritesTimer;

  private final Timer totalOperationsTimer;
  private final Timer successfulOperationsTimer;
  private final Timer failedOperationsTimer;

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

    totalStatementsTimer = registry.timer("total-statements-timer");
    successfulStatementsTimer = registry.timer("successful-statements-timer");
    failedStatementsTimer = registry.timer("failed-statements-timer");

    totalReadsTimer = registry.timer("total-reads-timer");
    successfulReadsTimer = registry.timer("successful-reads-timer");
    failedReadsTimer = registry.timer("failed-reads-timer");

    totalWritesTimer = registry.timer("total-writes-timer");
    successfulWritesTimer = registry.timer("successful-writes-timer");
    failedWritesTimer = registry.timer("failed-writes-timer");

    totalOperationsTimer = registry.timer("total-operations-timer");
    successfulOperationsTimer = registry.timer("successful-operations-timer");
    failedOperationsTimer = registry.timer("failed-operations-timer");
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
   * probably looking for {@link #getTotalWritesTimer()}.
   *
   * @return a {@link Timer} for total statement executions (successful and failed).
   */
  public Timer getTotalStatementsTimer() {
    return totalStatementsTimer;
  }

  /**
   * Returns a {@link Timer} for successful statement executions.
   *
   * <p>A batch statement is counted as one single statement. If that's not what you want, you are
   * probably looking for {@link #getSuccessfulWritesTimer()}.
   *
   * @return a {@link Timer} for successful statement executions.
   */
  public Timer getSuccessfulStatementsTimer() {
    return successfulStatementsTimer;
  }

  /**
   * Returns a {@link Timer} for failed statement executions.
   *
   * <p>A batch statement is counted as one single statement. If that's not what you want, you are
   * probably looking for {@link #getFailedWritesTimer()}.
   *
   * @return a {@link Timer} for failed statement executions.
   */
  public Timer getFailedStatementsTimer() {
    return failedStatementsTimer;
  }

  /**
   * Returns a {@link Timer} that evaluates the duration of execution of reads, both successful and
   * failed.
   *
   * @return a {@link Timer} that evaluates the duration of execution of reads, both successful and
   *     failed.
   */
  public Timer getTotalReadsTimer() {
    return totalReadsTimer;
  }

  /**
   * Returns a {@link Timer} that evaluates the duration of execution of successful reads.
   *
   * @return a {@link Timer} that evaluates the duration of execution of successful reads.
   */
  public Timer getSuccessfulReadsTimer() {
    return successfulReadsTimer;
  }

  /**
   * Returns a {@link Timer} that evaluates the duration of execution of failed reads.
   *
   * @return a {@link Timer} that evaluates the duration of execution of failed reads.
   */
  public Timer getFailedReadsTimer() {
    return failedReadsTimer;
  }

  /**
   * Returns a {@link Timer} that evaluates the duration of execution of writes, both successful and
   * failed.
   *
   * <p>A batch statement is counted as many times as the number of child statements it contains. If
   * that's not what you want, you are probably looking for {@link #getTotalStatementsTimer()}.
   *
   * @return a {@link Timer} that evaluates the duration of execution of writes, both successful and
   *     failed.
   */
  public Timer getTotalWritesTimer() {
    return totalWritesTimer;
  }

  /**
   * Returns a {@link Timer} that evaluates the duration of execution of successful writes.
   *
   * <p>A batch statement is counted as many times as the number of child statements it contains. If
   * that's not what you want, you are probably looking for {@link #getSuccessfulStatementsTimer()}.
   *
   * @return a {@link Timer} that evaluates the duration of execution of successful writes.
   */
  public Timer getSuccessfulWritesTimer() {
    return successfulWritesTimer;
  }

  /**
   * Returns a {@link Timer} that evaluates the duration of execution of failed writes.
   *
   * <p>A batch statement is counted as many times as the number of child statements it contains. If
   * that's not what you want, you are probably looking for {@link #getFailedStatementsTimer()}.
   *
   * @return a {@link Timer} that evaluates the duration of execution of failed writes.
   */
  public Timer getFailedWritesTimer() {
    return failedWritesTimer;
  }

  /**
   * Returns a {@link Timer} that evaluates the duration of execution of all operations, including
   * reads and writes, both successful and failed.
   *
   * @return a {@link Timer} that evaluates the duration of execution of all operations.
   */
  public Timer getTotalOperationsTimer() {
    return totalOperationsTimer;
  }

  /**
   * Returns a {@link Timer} that evaluates the duration of execution of all successful operations,
   * including reads and writes.
   *
   * @return a {@link Timer} that evaluates the duration of execution of all successful operations.
   */
  public Timer getSuccessfulOperationsTimer() {
    return successfulOperationsTimer;
  }

  /**
   * Returns a {@link Timer} that evaluates the duration of execution of all failed operations,
   * including reads and writes.
   *
   * @return a {@link Timer} that evaluates the duration of execution of all failed operations.
   */
  public Timer getFailedOperationsTimer() {
    return failedOperationsTimer;
  }

  @Override
  public void onExecutionStarted(Statement statement, ExecutionContext context) {
    start(context, totalStatementsTimer);
    start(context, successfulStatementsTimer);
    start(context, failedStatementsTimer);
    start(context, totalReadsTimer);
    start(context, successfulReadsTimer);
    start(context, failedReadsTimer);
    start(context, totalWritesTimer);
    start(context, successfulWritesTimer);
    start(context, failedWritesTimer);
    start(context, totalOperationsTimer);
    start(context, successfulOperationsTimer);
    start(context, failedOperationsTimer);
  }

  @Override
  public void onResultReceived(Result result, ExecutionContext context) {
    int delta = delta(result.getStatement());
    stop(context, totalOperationsTimer, delta);
    if (result instanceof ReadResult) {
      stop(context, totalReadsTimer, delta);
    } else {
      stop(context, totalWritesTimer, delta);
    }
    if (result.isSuccess()) {
      stop(context, successfulOperationsTimer, delta);
      if (result instanceof ReadResult) stop(context, successfulReadsTimer, delta);
      else stop(context, successfulWritesTimer, delta);
    } else {
      stop(context, failedOperationsTimer, delta);
      if (result instanceof ReadResult) stop(context, failedReadsTimer, delta);
      else stop(context, failedWritesTimer, delta);
    }
  }

  @Override
  public void onExecutionCompleted(Statement statement, ExecutionContext context) {
    stop(context, totalStatementsTimer, 1);
    stop(context, successfulStatementsTimer, 1);
  }

  @Override
  public void onExecutionFailed(BulkExecutionException exception, ExecutionContext context) {
    stop(context, totalStatementsTimer, 1);
    stop(context, failedStatementsTimer, 1);
  }

  private static void start(ExecutionContext context, Timer timer) {
    context.setAttribute(timer, timer.time());
  }

  private static void stop(ExecutionContext context, Timer timer, int delta) {
    Timer.Context timerContext =
        (Timer.Context) context.getAttribute(timer).orElseThrow(IllegalStateException::new);
    long elapsed = timerContext.stop();
    IntStream.range(1, delta).forEach(v -> timer.update(elapsed, NANOSECONDS));
  }

  private static int delta(Statement statement) {
    if (statement instanceof BatchStatement) return ((BatchStatement) statement).size();
    else return 1;
  }
}
