/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.tck;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.testng.Assert.fail;

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.dsbulk.executor.api.result.WriteResult;
import java.util.concurrent.Executor;

public abstract class WriteResultPublisherTestBase extends ResultPublisherTestBase<WriteResult> {

  @Override
  public long maxElementsFromPublisher() {
    return 1;
  }

  protected static Session setUpSuccessfulSession() {
    Session session = mock(Session.class);
    ResultSetFuture future = mock(ResultSetFuture.class);
    when(session.executeAsync(any(SimpleStatement.class))).thenReturn(future);
    ResultSet rs = mock(ResultSet.class);
    try {
      when(future.get()).thenReturn(rs);
    } catch (Exception e) {
      fail(e.getMessage(), e);
    }
    when(future.isDone()).thenReturn(true);
    when(rs.isFullyFetched()).thenReturn(true);
    ExecutionInfo executionInfo = mock(ExecutionInfo.class);
    when(rs.getExecutionInfo()).thenReturn(executionInfo);
    when(executionInfo.getPagingState()).thenReturn(null);
    doAnswer(
            invocation -> {
              ((Runnable) invocation.getArguments()[0]).run();
              return null;
            })
        .when(future)
        .addListener(any(Runnable.class), any(Executor.class));
    return session;
  }
}
