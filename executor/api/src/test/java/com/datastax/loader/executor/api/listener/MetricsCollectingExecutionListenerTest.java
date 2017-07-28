/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api.listener;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import com.datastax.driver.core.*;
import com.datastax.loader.executor.api.exception.BulkExecutionException;
import com.datastax.loader.executor.api.internal.listener.DefaultExecutionContext;
import com.datastax.loader.executor.api.internal.result.DefaultReadResult;
import com.datastax.loader.executor.api.internal.result.DefaultWriteResult;
import org.junit.Test;

/** */
public class MetricsCollectingExecutionListenerTest {

  private Statement successfulRead = new SimpleStatement("irrelevant");
  private Statement failedRead = new SimpleStatement("irrelevant");
  private Statement successfulWrite = new BatchStatement().add(successfulRead).add(failedRead);
  private Statement failedWrite = new BatchStatement().add(successfulRead).add(failedRead);

  @Test
  public void should_collect_metrics() throws Exception {

    MetricsCollectingExecutionListener listener = new MetricsCollectingExecutionListener();
    DefaultExecutionContext context = new DefaultExecutionContext();

    listener.onExecutionStarted(successfulRead, context);
    listener.onExecutionStarted(failedRead, context);
    listener.onExecutionStarted(successfulWrite, context);
    listener.onExecutionStarted(failedWrite, context);

    listener.onExecutionCompleted(successfulRead, context);
    listener.onExecutionFailed(
        new BulkExecutionException(new RuntimeException(), failedRead), context);
    listener.onExecutionCompleted(successfulWrite, context);
    listener.onExecutionFailed(
        new BulkExecutionException(new RuntimeException(), failedWrite), context);

    listener.onResultReceived(new DefaultReadResult(successfulRead, mock(Row.class)), context);
    listener.onResultReceived(new DefaultReadResult(successfulRead, mock(Row.class)), context);
    listener.onResultReceived(new DefaultReadResult(successfulRead, mock(Row.class)), context);
    listener.onResultReceived(
        new DefaultReadResult(new BulkExecutionException(new RuntimeException(), failedRead)),
        context);
    listener.onResultReceived(
        new DefaultWriteResult(successfulWrite, mock(ResultSet.class)), context);
    listener.onResultReceived(
        new DefaultWriteResult(new BulkExecutionException(new RuntimeException(), failedWrite)),
        context);

    // 3 successful reads
    // 1 failed read
    // 2 successful writes
    // 2 failed writes

    assertThat(listener.getTotalStatementsTimer().getCount()).isEqualTo(4);
    assertThat(listener.getFailedStatementsTimer().getCount()).isEqualTo(2);
    assertThat(listener.getSuccessfulStatementsTimer().getCount()).isEqualTo(2);

    assertThat(listener.getTotalOperationsTimer().getCount()).isEqualTo(8);
    assertThat(listener.getFailedOperationsTimer().getCount()).isEqualTo(3);
    assertThat(listener.getSuccessfulOperationsTimer().getCount()).isEqualTo(5);

    assertThat(listener.getTotalWritesTimer().getCount()).isEqualTo(4);
    assertThat(listener.getFailedWritesTimer().getCount()).isEqualTo(2);
    assertThat(listener.getSuccessfulWritesTimer().getCount()).isEqualTo(2);

    assertThat(listener.getTotalReadsTimer().getCount()).isEqualTo(4);
    assertThat(listener.getFailedReadsTimer().getCount()).isEqualTo(1);
    assertThat(listener.getSuccessfulReadsTimer().getCount()).isEqualTo(3);
  }
}
