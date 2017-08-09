/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api;

import static org.mockito.Matchers.any;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import org.junit.Before;

public abstract class AbstractNonContinuousBulkExecutorTest extends AbstractBulkExecutorTest {

  ResultSetFuture successFuture1a = mock(ResultSetFuture.class);
  ResultSetFuture successFuture1b = mock(ResultSetFuture.class);
  ResultSetFuture successFuture2a = mock(ResultSetFuture.class);
  ResultSetFuture failedFuture = mock(ResultSetFuture.class);

  ResultSet successfulResultSet1a = mock(ResultSet.class);
  ResultSet successfulResultSet1b = mock(ResultSet.class);
  ResultSet successfulResultSet2a = mock(ResultSet.class);

  Row row1aa = mock(Row.class);
  Row row1ab = mock(Row.class);
  Row row1ac = mock(Row.class);
  Row row1ba = mock(Row.class);
  Row row2aa = mock(Row.class);

  List<Row> page1a = Arrays.asList(row1aa, row1ab, row1ac);
  List<Row> page1b = Collections.singletonList(row1ba);
  List<Row> page2a = Collections.singletonList(row2aa);

  @Before
  public void setUpSession() throws ExecutionException, InterruptedException {
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
    when(successfulResultSet1a.iterator()).thenReturn(page1a.iterator());
    when(successfulResultSet1a.getAvailableWithoutFetching()).thenReturn(page1a.size());
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
    when(successfulResultSet1b.iterator()).thenReturn(page1b.iterator());
    when(successfulResultSet1b.getAvailableWithoutFetching()).thenReturn(page1b.size());
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
    when(successfulResultSet2a.iterator()).thenReturn(page2a.iterator());
    when(successfulResultSet2a.getAvailableWithoutFetching()).thenReturn(page2a.size());
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
