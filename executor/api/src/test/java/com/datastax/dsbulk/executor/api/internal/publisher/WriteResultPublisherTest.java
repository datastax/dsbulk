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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.dsbulk.executor.api.result.WriteResult;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import java.util.concurrent.CompletableFuture;
import org.reactivestreams.Publisher;

public class WriteResultPublisherTest extends ResultPublisherTestBase<WriteResult> {

  @Override
  public long maxElementsFromPublisher() {
    return 1;
  }

  @Override
  public Publisher<WriteResult> createPublisher(long elements) {
    Statement<?> statement = SimpleStatement.newInstance("irrelevant");
    CqlSession session = setUpSession();
    return new WriteResultPublisher(statement, session, true);
  }

  @Override
  public Publisher<WriteResult> createFailedPublisher() {
    Statement<?> statement = SimpleStatement.newInstance("irrelevant");
    CqlSession session = mock(CqlSession.class);
    return new WriteResultPublisher(statement, session, true, FAILED_LISTENER, null, null, null);
  }

  private static CqlSession setUpSession() {
    CqlSession session = mock(CqlSession.class);
    CompletableFuture<AsyncResultSet> future = new CompletableFuture<>();
    ExecutionInfo executionInfo = mock(ExecutionInfo.class);
    future.complete(new MockAsyncResultSet(0, executionInfo, null));
    when(session.executeAsync(any(SimpleStatement.class))).thenReturn(future);
    return session;
  }
}
