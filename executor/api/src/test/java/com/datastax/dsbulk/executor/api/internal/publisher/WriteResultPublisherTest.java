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

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import java.util.Optional;
import java.util.concurrent.Executor;
import org.reactivestreams.Publisher;

public class WriteResultPublisherTest extends ResultPublisherTestBase<WriteResult> {

  @Override
  public long maxElementsFromPublisher() {
    return 1;
  }

  @Override
  public Publisher<WriteResult> createPublisher(long elements) {
    Statement statement = new SimpleStatement("irrelevant");
    Session session = setUpSession();
    return new WriteResultPublisher(statement, session, true);
  }

  @Override
  public Publisher<WriteResult> createFailedPublisher() {
    Statement statement = new SimpleStatement("irrelevant");
    Session session = mock(Session.class);
    return new WriteResultPublisher(
        statement,
        session,
        true,
        FAILED_LISTENER,
        Optional.empty(),
        Optional.empty(),
        Optional.empty());
  }

  private static Session setUpSession() {
    Session session = mock(Session.class);
    ResultSetFuture future = mock(ResultSetFuture.class);
    ExecutionInfo executionInfo = mock(ExecutionInfo.class);
    when(future.isDone()).thenReturn(true);
    try {
      when(future.get()).thenReturn(new MockResultSet(0, executionInfo, null));
    } catch (Exception ignored) {
    }
    doAnswer(
            invocation -> {
              ((Runnable) invocation.getArguments()[0]).run();
              return null;
            })
        .when(future)
        .addListener(any(Runnable.class), any(Executor.class));
    when(session.executeAsync(any(SimpleStatement.class))).thenReturn(future);
    return session;
  }
}
