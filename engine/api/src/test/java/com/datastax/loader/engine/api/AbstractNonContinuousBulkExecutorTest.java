/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.api;

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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public abstract class AbstractNonContinuousBulkExecutorTest extends AbstractBulkExecutorTest {

  ResultSetFuture successFuture1 = mock(ResultSetFuture.class);
  ResultSetFuture successFuture2 = mock(ResultSetFuture.class);
  ResultSetFuture failedFuture = mock(ResultSetFuture.class);

  ResultSet successfulResultSet1 = mock(ResultSet.class);
  ResultSet successfulResultSet2 = mock(ResultSet.class);

  Row row1a = mock(Row.class);
  Row row1b = mock(Row.class);
  Row row1c = mock(Row.class);
  Row row1d = mock(Row.class);
  Row row2a = mock(Row.class);

  List<Row> rows1 = Arrays.asList(row1a, row1b, row1c, row1d);
  List<Row> rows2 = Collections.singletonList(row2a);

  @Before
  public void setUpSession() throws ExecutionException, InterruptedException {

    when(session.executeAsync(any(SimpleStatement.class)))
        .thenAnswer(
            invocation -> {
              SimpleStatement stmt = (SimpleStatement) invocation.getArguments()[0];
              switch (stmt.getQueryString()) {
                case "should succeed":
                  return successFuture1;
                case "should succeed 2":
                  return successFuture2;
                case "should fail":
                default:
                  return failedFuture;
              }
            });

    when(successFuture1.isDone()).thenReturn(true);
    when(successFuture2.isDone()).thenReturn(true);
    when(failedFuture.isDone()).thenReturn(true);

    doAnswer(
            invocation -> {
              ((Runnable) invocation.getArguments()[0]).run();
              return null;
            })
        .when(successFuture1)
        .addListener(any(Runnable.class), any(Executor.class));
    doAnswer(
            invocation -> {
              ((Runnable) invocation.getArguments()[0]).run();
              return null;
            })
        .when(successFuture2)
        .addListener(any(Runnable.class), any(Executor.class));
    doAnswer(
            invocation -> {
              ((Runnable) invocation.getArguments()[0]).run();
              return null;
            })
        .when(failedFuture)
        .addListener(any(Runnable.class), any(Executor.class));

    // request 1 (also used for write tests, but the resultset is not consumed)
    when(successFuture1.get()).thenReturn(successfulResultSet1);
    when(successfulResultSet1.iterator()).thenReturn(rows1.iterator());
    when(successfulResultSet1.isFullyFetched()).thenReturn(true);
    ExecutionInfo executionInfo1a = mock(ExecutionInfo.class);
    when(successfulResultSet1.getExecutionInfo()).thenReturn(executionInfo1a);
    PagingState ps = mock(PagingState.class);
    when(executionInfo1a.getPagingState()).thenReturn(ps);

    // request 2
    when(successFuture2.get()).thenReturn(successfulResultSet2);
    when(successfulResultSet2.iterator()).thenReturn(rows2.iterator());
    when(successfulResultSet2.isFullyFetched()).thenReturn(true);
    when(successfulResultSet2.getAvailableWithoutFetching()).thenReturn(rows2.size());
    ExecutionInfo executionInfo2a = mock(ExecutionInfo.class);
    when(successfulResultSet2.getExecutionInfo()).thenReturn(executionInfo2a);
    when(executionInfo2a.getPagingState()).thenReturn(null);

    // failed request
    when(failedFuture.get())
        .thenThrow(
            new ExecutionException(
                new SyntaxError(
                    InetSocketAddress.createUnresolved("localhost", 9042),
                    "line 1:0 no viable alternative at input 'should' ([should]...)")));

    Cluster cluster = mock(Cluster.class);
    when(session.getCluster()).thenReturn(cluster);
    Configuration configuration = mock(Configuration.class);
    when(cluster.getConfiguration()).thenReturn(configuration);
    when(configuration.getQueryOptions()).thenReturn(new QueryOptions().setFetchSize(100));
  }
}
