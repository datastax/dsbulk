/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.tck;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

import com.datastax.driver.core.AsyncContinuousPagingResult;
import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.google.common.util.concurrent.ListenableFuture;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;

public abstract class ContinuousReadResultPublisherTestBase
    extends ResultPublisherTestBase<ReadResult> {

  protected static ContinuousPagingSession setUpSuccessfulSession(long elements) {
    ContinuousPagingSession session = mock(ContinuousPagingSession.class);
    @SuppressWarnings("unchecked")
    ListenableFuture<AsyncContinuousPagingResult> future = mock(ListenableFuture.class);
    when(session.executeContinuouslyAsync(
            any(SimpleStatement.class), any(ContinuousPagingOptions.class)))
        .thenReturn(future);
    AsyncContinuousPagingResult page = mock(AsyncContinuousPagingResult.class);
    try {
      when(future.get()).thenReturn(page);
    } catch (Exception e) {
      fail(e.getMessage(), e);
    }
    when(future.isDone()).thenReturn(true);
    when(page.currentPage())
        .thenAnswer(invocation -> ROWS.subList(0, Math.toIntExact(Math.min(100, elements))));
    ExecutionInfo executionInfo = mock(ExecutionInfo.class);
    when(page.getExecutionInfo()).thenReturn(executionInfo);
    when(page.isLast()).thenReturn(true);
    doAnswer(
            invocation -> {
              ((Runnable) invocation.getArguments()[0]).run();
              return null;
            })
        .when(future)
        .addListener(any(Runnable.class), any(Executor.class));
    setUpCluster(session);
    return session;
  }

  protected static ContinuousPagingSession setUpFailedSession() {
    ContinuousPagingSession session = mock(ContinuousPagingSession.class);
    @SuppressWarnings("unchecked")
    ListenableFuture<AsyncContinuousPagingResult> future = mock(ListenableFuture.class);
    when(session.executeContinuouslyAsync(
            any(SimpleStatement.class), any(ContinuousPagingOptions.class)))
        .thenReturn(future);
    try {
      when(future.get())
          .thenThrow(
              new ExecutionException(
                  new SyntaxError(
                      InetSocketAddress.createUnresolved("localhost", 9042),
                      "line 1:0 no viable alternative at input 'should' ([should]...)")));
    } catch (Exception e) {
      fail(e.getMessage(), e);
    }
    when(future.isDone()).thenReturn(true);
    doAnswer(
            invocation -> {
              ((Runnable) invocation.getArguments()[0]).run();
              return null;
            })
        .when(future)
        .addListener(any(Runnable.class), any(Executor.class));
    setUpCluster(session);
    return session;
  }
}
