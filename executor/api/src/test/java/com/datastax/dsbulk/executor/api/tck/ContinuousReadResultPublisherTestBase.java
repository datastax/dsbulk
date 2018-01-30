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

import com.datastax.driver.core.AsyncContinuousPagingResult;
import com.datastax.driver.core.ContinuousPagingOptions;
import com.datastax.driver.core.ContinuousPagingSession;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.google.common.util.concurrent.ListenableFuture;
import io.reactivex.Flowable;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executor;
import org.reactivestreams.tck.PublisherVerification;
import org.reactivestreams.tck.TestEnvironment;

public abstract class ContinuousReadResultPublisherTestBase
    extends PublisherVerification<ReadResult> {

  public ContinuousReadResultPublisherTestBase() {
    super(new TestEnvironment());
  }

  protected static ContinuousPagingSession setUpSuccessfulSession(long elements) {
    ContinuousPagingSession session = mock(ContinuousPagingSession.class);
    ListenableFuture<AsyncContinuousPagingResult> previous = mockPages(elements);
    when(session.executeContinuouslyAsync(
            any(SimpleStatement.class), any(ContinuousPagingOptions.class)))
        .thenReturn(previous);
    return session;
  }

  private static ListenableFuture<AsyncContinuousPagingResult> mockPages(long elements) {
    // The TCK usually requests between 0 and 20 items, or Long.MAX_VALUE.
    // Past 3 elements it never checks how many elements have been effectively produced,
    // so we can safely cap at, say, 20.
    int effective = (int) Math.min(elements, 20L);
    ListenableFuture<AsyncContinuousPagingResult> previous = null;
    if (effective > 0) {
      // create pages of 5 elements each to exercise pagination
      List<Integer> pages =
          Flowable.range(0, effective).buffer(5).map(List::size).toList().blockingGet();
      Collections.reverse(pages);
      for (Integer size : pages) {
        previous = mockPage(previous, size);
      }
    } else {
      previous = mockPage(null, 0);
    }
    return previous;
  }

  private static ListenableFuture<AsyncContinuousPagingResult> mockPage(
      ListenableFuture<AsyncContinuousPagingResult> previous, int size) {
    @SuppressWarnings("unchecked")
    ListenableFuture<AsyncContinuousPagingResult> future = mock(ListenableFuture.class);
    ExecutionInfo executionInfo = mock(ExecutionInfo.class);
    when(executionInfo.getPagingState())
        .thenReturn(previous == null ? null : mock(PagingState.class));
    when(future.isDone()).thenReturn(true);
    try {
      when(future.get()).thenReturn(new MockResultSet(size, executionInfo, previous));
    } catch (Exception ignored) {
    }
    doAnswer(
            invocation -> {
              ((Runnable) invocation.getArguments()[0]).run();
              return null;
            })
        .when(future)
        .addListener(any(Runnable.class), any(Executor.class));
    previous = future;
    return previous;
  }
}
