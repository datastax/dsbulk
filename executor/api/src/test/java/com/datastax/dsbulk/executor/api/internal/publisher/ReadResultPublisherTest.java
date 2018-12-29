/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.internal.publisher;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import io.reactivex.Flowable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executor;
import org.reactivestreams.Publisher;

public class ReadResultPublisherTest extends ResultPublisherTestBase<ReadResult> {

  private static final int PAGE_SIZE = 5;

  @Override
  public Publisher<ReadResult> createPublisher(long elements) {
    Statement statement = new SimpleStatement("irrelevant");
    Session session = setUpSession(elements);
    return new ReadResultPublisher(
        statement, session, Optional.empty(), Optional.empty(), Optional.empty(), true);
  }

  @Override
  public Publisher<ReadResult> createFailedPublisher() {
    Statement statement = new SimpleStatement("irrelevant");
    Session session = setUpSession(1);
    return new ReadResultPublisher(
        statement, session, FAILED_LISTENER, Optional.empty(), Optional.empty(), true);
  }

  private static Session setUpSession(long elements) {
    Session session = mock(Session.class);
    Cluster cluster = mock(Cluster.class);
    when(session.getCluster()).thenReturn(cluster);
    Configuration configuration = mock(Configuration.class);
    when(cluster.getConfiguration()).thenReturn(configuration);
    QueryOptions queryOptions = mock(QueryOptions.class);
    when(configuration.getQueryOptions()).thenReturn(queryOptions);
    ProtocolOptions protocolOptions = mock(ProtocolOptions.class);
    when(configuration.getProtocolOptions()).thenReturn(protocolOptions);
    when(queryOptions.getFetchSize()).thenReturn(PAGE_SIZE);
    when(protocolOptions.getProtocolVersion()).thenReturn(ProtocolVersion.V4);
    ResultSetFuture previous = mockPages(elements);
    when(session.executeAsync(any(SimpleStatement.class))).thenReturn(previous);
    return session;
  }

  private static ResultSetFuture mockPages(long elements) {
    // The TCK usually requests between 0 and 20 items, or Long.MAX_VALUE.
    // Past 3 elements it never checks how many elements have been effectively produced,
    // so we can safely cap at, say, 20.
    int effective = (int) Math.min(elements, 20L);
    ResultSetFuture previous = null;
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

  private static ResultSetFuture mockPage(ResultSetFuture previous, int size) {
    ResultSetFuture future = mock(ResultSetFuture.class);
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
