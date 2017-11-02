/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.internal.listener.DefaultExecutionContext;
import org.junit.Test;

/** */
public class MetricsCollectingExecutionListenerTest {

  private final Statement successfulRead = new SimpleStatement("irrelevant");
  private final Statement failedRead = new SimpleStatement("irrelevant");

  private final Statement successfulWrite =
      new BatchStatement()
          .add(new SimpleStatement("irrelevant"))
          .add(new SimpleStatement("irrelevant"));
  private final Statement failedWrite =
      new BatchStatement()
          .add(new SimpleStatement("irrelevant"))
          .add(new SimpleStatement("irrelevant"));
  private final Row row = mock(Row.class);

  @Test
  public void should_collect_metrics() throws Exception {

    MetricsCollectingExecutionListener listener = new MetricsCollectingExecutionListener();

    ExecutionContext global = new TestExecutionContext();
    ExecutionContext local1 = new TestExecutionContext();
    ExecutionContext local2 = new TestExecutionContext();
    ExecutionContext local3 = new TestExecutionContext();
    ExecutionContext local4 = new TestExecutionContext();

    listener.onExecutionStarted(successfulRead, global);
    listener.onExecutionStarted(failedRead, global);
    listener.onExecutionStarted(successfulWrite, global);
    listener.onExecutionStarted(failedWrite, global);

    assertThat(listener.getInFlightRequestsCounter().getCount()).isEqualTo(0);

    listener.onReadRequestStarted(successfulRead, local1);
    assertThat(listener.getInFlightRequestsCounter().getCount()).isEqualTo(1);
    listener.onReadRequestStarted(failedRead, local2);
    assertThat(listener.getInFlightRequestsCounter().getCount()).isEqualTo(2);
    listener.onWriteRequestStarted(successfulWrite, local3);
    assertThat(listener.getInFlightRequestsCounter().getCount()).isEqualTo(3);
    listener.onWriteRequestStarted(failedWrite, local4);
    assertThat(listener.getInFlightRequestsCounter().getCount()).isEqualTo(4);

    listener.onReadRequestSuccessful(successfulRead, local1);
    // simulate 3 rows received
    listener.onRowReceived(row, local1);
    listener.onRowReceived(row, local1);
    listener.onRowReceived(row, local1);
    assertThat(listener.getInFlightRequestsCounter().getCount()).isEqualTo(3);
    listener.onReadRequestFailed(failedRead, new RuntimeException(), local2);
    assertThat(listener.getInFlightRequestsCounter().getCount()).isEqualTo(2);
    listener.onWriteRequestSuccessful(successfulWrite, local3);
    assertThat(listener.getInFlightRequestsCounter().getCount()).isEqualTo(1);
    listener.onWriteRequestFailed(failedWrite, new RuntimeException(), local4);
    assertThat(listener.getInFlightRequestsCounter().getCount()).isEqualTo(0);

    listener.onExecutionSuccessful(successfulRead, global);
    listener.onExecutionFailed(
        new BulkExecutionException(new RuntimeException(), failedRead), global);
    listener.onExecutionSuccessful(successfulWrite, global);
    listener.onExecutionFailed(
        new BulkExecutionException(new RuntimeException(), failedWrite), global);

    // 3 successful reads
    // 1 failed read
    // 2 successful writes
    // 2 failed writes

    assertThat(listener.getStatementsTimer().getCount()).isEqualTo(4);
    assertThat(listener.getFailedStatementsCounter().getCount()).isEqualTo(2);
    assertThat(listener.getSuccessfulStatementsCounter().getCount()).isEqualTo(2);

    assertThat(listener.getReadsWritesTimer().getCount()).isEqualTo(8);
    assertThat(listener.getFailedReadsWritesCounter().getCount()).isEqualTo(3);
    assertThat(listener.getSuccessfulReadsWritesCounter().getCount()).isEqualTo(5);

    assertThat(listener.getWritesTimer().getCount()).isEqualTo(4);
    assertThat(listener.getFailedWritesCounter().getCount()).isEqualTo(2);
    assertThat(listener.getSuccessfulWritesCounter().getCount()).isEqualTo(2);

    assertThat(listener.getReadsTimer().getCount()).isEqualTo(4);
    assertThat(listener.getFailedReadsCounter().getCount()).isEqualTo(1);
    assertThat(listener.getSuccessfulReadsCounter().getCount()).isEqualTo(3);
  }

  private static class TestExecutionContext extends DefaultExecutionContext {
    @Override
    public long elapsedTimeNanos() {
      return 42;
    }
  }
}
