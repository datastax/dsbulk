/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.PagingState;
import com.datastax.driver.core.QueryOptions;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.exceptions.SyntaxError;
import java.net.InetSocketAddress;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import org.junit.jupiter.api.BeforeEach;

public abstract class NonContinuousBulkExecutorTestBase extends BulkExecutorTestBase {

  private ResultSetFuture successFuture1a = mock(ResultSetFuture.class);
  private ResultSetFuture successFuture1b = mock(ResultSetFuture.class);
  private ResultSetFuture successFuture2a = mock(ResultSetFuture.class);
  private ResultSetFuture failedFuture = mock(ResultSetFuture.class);

  private ResultSet successfulResultSet1a = mock(ResultSet.class);
  private ResultSet successfulResultSet1b = mock(ResultSet.class);
  private ResultSet successfulResultSet2a = mock(ResultSet.class);

  private Row row1aa = mock(Row.class);
  private Row row1ab = mock(Row.class);
  private Row row1ac = mock(Row.class);
  private Row row1ba = mock(Row.class);
  private Row row2aa = mock(Row.class);

  private List<Row> page1a = Arrays.asList(row1aa, row1ab, row1ac);
  private List<Row> page1b = Collections.singletonList(row1ba);
  private List<Row> page2a = Collections.singletonList(row2aa);

  @BeforeEach
  void setUpSession() throws ExecutionException, InterruptedException {
    when(session.executeAsync(any(SimpleStatement.class)))
        .thenAnswer(
            invocation -> {
              SimpleStatement stmt = (SimpleStatement) invocation.getArguments()[0];
              switch (stmt.getQueryString()) {
                case "should succeed":
                  return successFuture1a;
                case "should succeed 2":
                  return successFuture2a;
                case "should fail":
                default:
                  return failedFuture;
              }
            });

    // request 1a (also used for write tests, but the resultset is not consumed)
    when(successFuture1a.get()).thenReturn(successfulResultSet1a);
    when(successFuture1a.isDone()).thenReturn(true);
    ArrayDeque<Row> queue1a = new ArrayDeque<>(page1a);
    when(successfulResultSet1a.one()).thenAnswer(invocation -> queue1a.poll());
    when(successfulResultSet1a.isFullyFetched()).thenReturn(true); // only useful in write tests
    ExecutionInfo executionInfo1a = mock(ExecutionInfo.class);
    when(successfulResultSet1a.getExecutionInfo()).thenReturn(executionInfo1a);
    PagingState ps = mock(PagingState.class);
    when(executionInfo1a.getPagingState()).thenReturn(ps);
    when(successfulResultSet1a.fetchMoreResults()).thenReturn(successFuture1b);
    doAnswer(
            invocation -> {
              ((Runnable) invocation.getArguments()[0]).run();
              return null;
            })
        .when(successFuture1a)
        .addListener(any(Runnable.class), any(Executor.class));

    // request 1b
    when(successFuture1b.get()).thenReturn(successfulResultSet1b);
    when(successFuture1b.isDone()).thenReturn(true);
    ArrayDeque<Row> queue1b = new ArrayDeque<>(page1b);
    when(successfulResultSet1b.one()).thenAnswer(invocation -> queue1b.poll());
    when(successfulResultSet1b.isFullyFetched()).thenReturn(true); // only useful in write tests
    ExecutionInfo executionInfo1b = mock(ExecutionInfo.class);
    when(successfulResultSet1b.getExecutionInfo()).thenReturn(executionInfo1b);
    when(executionInfo1b.getPagingState()).thenReturn(null);
    doAnswer(
            invocation -> {
              ((Runnable) invocation.getArguments()[0]).run();
              return null;
            })
        .when(successFuture1b)
        .addListener(any(Runnable.class), any(Executor.class));

    // request 2a
    when(successFuture2a.get()).thenReturn(successfulResultSet2a);
    when(successFuture2a.isDone()).thenReturn(true);
    ArrayDeque<Row> queue2a = new ArrayDeque<>(page2a);
    when(successfulResultSet2a.one()).thenAnswer(invocation -> queue2a.poll());
    when(successfulResultSet2a.isFullyFetched()).thenReturn(true); // only useful in write tests
    ExecutionInfo executionInfo2a = mock(ExecutionInfo.class);
    when(successfulResultSet2a.getExecutionInfo()).thenReturn(executionInfo2a);
    when(executionInfo2a.getPagingState()).thenReturn(null);
    doAnswer(
            invocation -> {
              ((Runnable) invocation.getArguments()[0]).run();
              return null;
            })
        .when(successFuture2a)
        .addListener(any(Runnable.class), any(Executor.class));

    // failed request
    when(failedFuture.get())
        .thenThrow(
            new ExecutionException(
                new SyntaxError(
                    InetSocketAddress.createUnresolved("localhost", 9042),
                    "line 1:0 no viable alternative at input 'should' ([should]...)")));
    when(failedFuture.isDone()).thenReturn(true);
    doAnswer(
            invocation -> {
              ((Runnable) invocation.getArguments()[0]).run();
              return null;
            })
        .when(failedFuture)
        .addListener(any(Runnable.class), any(Executor.class));

    Cluster cluster = mock(Cluster.class);
    when(session.getCluster()).thenReturn(cluster);
    Configuration configuration = mock(Configuration.class);
    when(cluster.getConfiguration()).thenReturn(configuration);
    when(configuration.getQueryOptions()).thenReturn(new QueryOptions().setFetchSize(100));
  }
}
