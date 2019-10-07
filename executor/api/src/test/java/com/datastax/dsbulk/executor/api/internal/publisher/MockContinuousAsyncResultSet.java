/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.internal.publisher;

import com.datastax.dse.driver.api.core.cql.continuous.ContinuousAsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.ExecutionInfo;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.internal.core.cql.EmptyColumnDefinitions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class MockContinuousAsyncResultSet implements ContinuousAsyncResultSet {

  private final List<Row> rows;
  private final Iterator<Row> iterator;
  private final CompletionStage<ContinuousAsyncResultSet> nextPage;
  private final ExecutionInfo executionInfo;
  private int remaining;

  MockContinuousAsyncResultSet(
      int size, ExecutionInfo executionInfo, CompletionStage<ContinuousAsyncResultSet> nextPage) {
    rows = IntStream.range(0, size).boxed().map(MockRow::new).collect(Collectors.toList());
    this.executionInfo = executionInfo;
    iterator = rows.iterator();
    remaining = size;
    this.nextPage = nextPage;
  }

  @Override
  public Row one() {
    if (!iterator.hasNext()) {
      return null;
    }
    Row next = iterator.next();
    remaining--;
    return next;
  }

  @Override
  @NonNull
  public Iterable<Row> currentPage() {
    return rows;
  }

  @Override
  @NonNull
  public CompletionStage<ContinuousAsyncResultSet> fetchNextPage() throws IllegalStateException {
    return nextPage;
  }

  @Override
  public int remaining() {
    return remaining;
  }

  @Override
  public boolean hasMorePages() {
    return nextPage != null;
  }

  @Override
  public int pageNumber() {
    return 0;
  }

  @Override
  public void cancel() {}

  @Override
  @NonNull
  public ExecutionInfo getExecutionInfo() {
    return executionInfo;
  }

  @Override
  @NonNull
  public ColumnDefinitions getColumnDefinitions() {
    return EmptyColumnDefinitions.INSTANCE;
  }

  @Override
  public boolean wasApplied() {
    return true;
  }
}
