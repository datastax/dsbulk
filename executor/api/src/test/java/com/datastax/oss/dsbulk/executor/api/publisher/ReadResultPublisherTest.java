/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.executor.api.publisher;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
import io.reactivex.Flowable;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.reactivestreams.Publisher;

public class ReadResultPublisherTest extends ResultPublisherTestBase<ReadResult> {

  private static final int PAGE_SIZE = 5;

  @Override
  public Publisher<ReadResult> createPublisher(long elements) {
    Statement<?> statement = SimpleStatement.newInstance("irrelevant");
    CqlSession session = setUpSession(elements);
    return new ReadResultPublisher(statement, session, true);
  }

  @Override
  public Publisher<ReadResult> createFailedPublisher() {
    Statement<?> statement = SimpleStatement.newInstance("irrelevant");
    CqlSession session = setUpSession(1);
    return new ReadResultPublisher(statement, session, true, FAILED_LISTENER, null, null, null);
  }

  private static CqlSession setUpSession(long elements) {
    CqlSession session = mock(CqlSession.class);
    CompletionStage<AsyncResultSet> previous = mockPages(elements);
    when(session.executeAsync(any(SimpleStatement.class))).thenReturn(previous);
    return session;
  }

  private static CompletionStage<AsyncResultSet> mockPages(long elements) {
    // The TCK usually requests between 0 and 20 items, or Long.MAX_VALUE.
    // Past 3 elements it never checks how many elements have been effectively produced,
    // so we can safely cap at, say, 20.
    int effective = (int) Math.min(elements, 20L);
    CompletionStage<AsyncResultSet> previous = null;
    if (effective > 0) {
      // create pages of 5 elements each to exercise pagination
      List<Integer> pages =
          Flowable.range(0, effective).buffer(PAGE_SIZE).map(List::size).toList().blockingGet();
      Collections.reverse(pages);
      for (Integer size : pages) {
        previous = mockPage(previous, size);
      }
    } else {
      previous = mockPage(null, 0);
    }
    return previous;
  }

  private static CompletionStage<AsyncResultSet> mockPage(
      CompletionStage<AsyncResultSet> previous, int size) {
    CompletableFuture<AsyncResultSet> future = new CompletableFuture<>();
    ExecutionInfo executionInfo = mock(ExecutionInfo.class);
    when(executionInfo.getPagingState())
        .thenReturn(previous == null ? null : ByteBuffer.wrap(new byte[] {1}));
    future.complete(new MockAsyncResultSet(size, executionInfo, previous));
    previous = future;
    return previous;
  }
}
