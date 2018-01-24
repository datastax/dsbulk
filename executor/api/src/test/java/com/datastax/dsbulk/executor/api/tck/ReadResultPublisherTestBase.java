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

import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.SyntaxError;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import java.net.InetSocketAddress;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public abstract class ReadResultPublisherTestBase extends ResultPublisherTestBase<ReadResult> {

  protected static Session setUpSuccessfulSession(long elements) {
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
    when(rs.iterator()).thenAnswer(invocation -> ROWS.iterator());
    when(rs.getAvailableWithoutFetching())
        .thenReturn(elements > Integer.MAX_VALUE ? Integer.MAX_VALUE : (int) elements);
    ExecutionInfo executionInfo = mock(ExecutionInfo.class);
    when(rs.getExecutionInfo()).thenReturn(executionInfo);
    when(executionInfo.getPagingState()).thenReturn(null);
    ExecutorService executor = Executors.newSingleThreadExecutor();
    doAnswer(
            invocation -> {
              executor.submit((Runnable) invocation.getArguments()[0]);
              return null;
            })
        .when(future)
        .addListener(any(Runnable.class), any(Executor.class));
    setUpCluster(session);
    return session;
  }

  public static Session setUpFailedSession() {
    Session session = mock(Session.class);
    ResultSetFuture future = mock(ResultSetFuture.class);
    when(session.executeAsync(any(SimpleStatement.class))).thenReturn(future);
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
