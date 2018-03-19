/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.tck;

import com.datastax.driver.core.AsyncContinuousPagingResult;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.google.common.util.concurrent.ListenableFuture;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

class MockResultSet implements ResultSet, AsyncContinuousPagingResult {

  private final List<Row> rows;
  private final Iterator<Row> iterator;
  private final ListenableFuture<?> nextPage;
  private final ExecutionInfo executionInfo;
  private int remaining;

  MockResultSet(int size, ExecutionInfo executionInfo, ListenableFuture<?> nextPage) {
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
  public List<Row> all() {
    return rows;
  }

  @Override
  public Iterator<Row> iterator() {
    return iterator;
  }

  @Override
  public Iterable<Row> currentPage() {
    return rows;
  }

  @Override
  public boolean isLast() {
    return nextPage == null;
  }

  @Override
  public boolean isExhausted() {
    return nextPage == null;
  }

  @Override
  public boolean isFullyFetched() {
    return !iterator.hasNext() && isExhausted();
  }

  @Override
  public int getAvailableWithoutFetching() {
    return remaining;
  }

  @SuppressWarnings("unchecked")
  @Override
  public ListenableFuture<AsyncContinuousPagingResult> nextPage() {
    return (ListenableFuture<AsyncContinuousPagingResult>) nextPage;
  }

  @SuppressWarnings("unchecked")
  @Override
  public ListenableFuture<ResultSet> fetchMoreResults() {
    return (ListenableFuture<ResultSet>) nextPage;
  }

  @Override
  public ExecutionInfo getExecutionInfo() {
    return executionInfo;
  }

  @Override
  public int pageNumber() {
    return 0;
  }

  @Override
  public List<ExecutionInfo> getAllExecutionInfo() {
    return null;
  }

  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return null;
  }

  @Override
  public void cancel() {}

  @Override
  public boolean wasApplied() {
    return true;
  }
}
