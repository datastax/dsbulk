/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.tests.driver;

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

public final class MockContinuousAsyncResultSet implements ContinuousAsyncResultSet {

  private final List<Row> rows;
  private final Iterator<Row> iterator;
  private final CompletionStage<ContinuousAsyncResultSet> nextPage;
  private final ExecutionInfo executionInfo;
  private int remaining;

  public MockContinuousAsyncResultSet(
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
