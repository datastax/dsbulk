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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.servererrors.SyntaxError;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import org.junit.jupiter.api.BeforeEach;

public abstract class NonContinuousBulkExecutorTestBase extends BulkExecutorTestBase {

  private CompletableFuture<AsyncResultSet> futureReadSuccess1a = new CompletableFuture<>();
  private CompletableFuture<AsyncResultSet> futureReadSuccess1b = new CompletableFuture<>();
  private CompletableFuture<AsyncResultSet> futureReadSuccess2a = new CompletableFuture<>();

  private CompletableFuture<AsyncResultSet> futureWriteSuccess1 = new CompletableFuture<>();
  private CompletableFuture<AsyncResultSet> futureWriteSuccess2 = new CompletableFuture<>();

  private CompletableFuture<AsyncResultSet> failedFuture = new CompletableFuture<>();

  private AsyncResultSet successfulReadResultSet1a = mock(AsyncResultSet.class, "page1a");
  private AsyncResultSet successfulReadResultSet1b = mock(AsyncResultSet.class, "page1b");
  private AsyncResultSet successfulReadResultSet2a = mock(AsyncResultSet.class, "page2a");

  private AsyncResultSet successfulWriteResultSet1 = mock(AsyncResultSet.class, "page1");
  private AsyncResultSet successfulWriteResultSet2 = mock(AsyncResultSet.class, "page2");

  private Row row1aa = mock(Row.class);
  private Row row1ab = mock(Row.class);
  private Row row1ac = mock(Row.class);
  private Row row1ba = mock(Row.class);
  private Row row2aa = mock(Row.class);

  private List<Row> page1a = Arrays.asList(row1aa, row1ab, row1ac);
  private List<Row> page1b = Collections.singletonList(row1ba);
  private List<Row> page2a = Collections.singletonList(row2aa);

  private ByteBuffer pagingState = ByteBuffer.wrap(new byte[] {1});

  @BeforeEach
  void setUpSession() {
    when(session.executeAsync(any(SimpleStatement.class)))
        .thenAnswer(
            invocation -> {
              SimpleStatement stmt = (SimpleStatement) invocation.getArguments()[0];
              switch (stmt.getQuery()) {
                case "read should succeed 1":
                  return futureReadSuccess1a;
                case "read should succeed 2":
                  return futureReadSuccess2a;
                case "write should succeed 1":
                  return futureWriteSuccess1;
                case "write should succeed 2":
                  return futureWriteSuccess2;
                case "should fail":
                default:
                  return failedFuture;
              }
            });

    // read request 1a
    futureReadSuccess1a.complete(successfulReadResultSet1a);
    when(successfulReadResultSet1a.currentPage()).thenReturn(page1a);
    when(successfulReadResultSet1a.hasMorePages()).thenReturn(true);
    ExecutionInfo executionInfo1a = mock(ExecutionInfo.class);
    when(successfulReadResultSet1a.getExecutionInfo()).thenReturn(executionInfo1a);
    when(executionInfo1a.getPagingState()).thenReturn(pagingState);
    //noinspection unchecked
    when(successfulReadResultSet1a.fetchNextPage())
        .thenReturn((CompletionStage) futureReadSuccess1b);

    // read request 1b
    futureReadSuccess1b.complete(successfulReadResultSet1b);
    when(successfulReadResultSet1b.currentPage()).thenReturn(page1b);
    when(successfulReadResultSet1b.hasMorePages()).thenReturn(false);
    ExecutionInfo executionInfo1b = mock(ExecutionInfo.class);
    when(successfulReadResultSet1b.getExecutionInfo()).thenReturn(executionInfo1b);
    when(executionInfo1b.getPagingState()).thenReturn(null);

    // read request 2a
    futureReadSuccess2a.complete(successfulReadResultSet2a);
    when(successfulReadResultSet2a.currentPage()).thenReturn(page2a);
    when(successfulReadResultSet2a.hasMorePages()).thenReturn(false);
    ExecutionInfo executionInfo2a = mock(ExecutionInfo.class);
    when(successfulReadResultSet2a.getExecutionInfo()).thenReturn(executionInfo2a);
    when(executionInfo2a.getPagingState()).thenReturn(null);

    // write request 1
    futureWriteSuccess1.complete(successfulWriteResultSet1);
    when(successfulWriteResultSet1.currentPage()).thenReturn(Collections.emptyList());
    when(successfulWriteResultSet1.hasMorePages()).thenReturn(false);
    when(successfulWriteResultSet1.getExecutionInfo()).thenReturn(executionInfo1a);
    when(executionInfo1a.getPagingState()).thenReturn(null);

    // write request 2
    futureWriteSuccess2.complete(successfulWriteResultSet2);
    when(successfulWriteResultSet2.currentPage()).thenReturn(Collections.emptyList());
    when(successfulWriteResultSet2.hasMorePages()).thenReturn(false);
    when(successfulWriteResultSet2.getExecutionInfo()).thenReturn(executionInfo2a);
    when(executionInfo2a.getPagingState()).thenReturn(null);

    // failed request
    failedFuture.completeExceptionally(
        new SyntaxError(
            mock(Node.class), "line 1:0 no viable alternative at input 'should' ([should]...)"));
  }
}
