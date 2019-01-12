/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.internal.subscription;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.internal.result.DefaultReadResult;
import com.datastax.dsbulk.executor.api.listener.ExecutionContext;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.RateLimiter;
import java.util.concurrent.Semaphore;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Subscriber;

@SuppressWarnings("UnstableApiUsage")
public class ReadResultSubscription extends ResultSubscription<ReadResult, ResultSet> {

  private final int pageSize;

  public ReadResultSubscription(
      @NotNull Subscriber<? super ReadResult> subscriber,
      @NotNull Statement statement,
      @Nullable ExecutionListener listener,
      @Nullable Semaphore maxConcurrentRequests,
      @Nullable Semaphore maxConcurrentQueries,
      @Nullable RateLimiter rateLimiter,
      boolean failFast,
      int pageSize) {
    super(
        subscriber,
        statement,
        listener,
        maxConcurrentRequests,
        maxConcurrentQueries,
        rateLimiter,
        failFast);
    this.pageSize = pageSize;
  }

  @Override
  Page toPage(ResultSet rs, ExecutionContext local) {
    return new Page(
        new ReadResultIterator(rs, local), isLastPage(rs) ? rs::fetchMoreResults : null);
  }

  @Override
  ReadResult toErrorResult(BulkExecutionException error) {
    return new DefaultReadResult(error);
  }

  @Override
  void onRequestStarted(ExecutionContext local) {
    if (listener != null) {
      listener.onReadRequestStarted(statement, local);
    }
  }

  @Override
  void onRequestSuccessful(ResultSet rs, ExecutionContext local) {
    if (listener != null) {
      listener.onReadRequestSuccessful(statement, local);
    }
  }

  @Override
  void onRequestFailed(Throwable t, ExecutionContext local) {
    if (listener != null) {
      listener.onReadRequestFailed(statement, t, local);
    }
  }

  @Override
  boolean isLastPage(ResultSet page) {
    return page.getExecutionInfo().getPagingState() != null;
  }

  @Override
  void onBeforeResultEmitted(ReadResult result) {
    if (rateLimiter != null) {
      rateLimiter.acquire();
    }
  }

  private class ReadResultIterator extends AbstractIterator<ReadResult> {

    private final ResultSet rs;
    private final ExecutionContext local;
    private int remaining;

    public ReadResultIterator(ResultSet rs, ExecutionContext local) {
      this.rs = rs;
      this.remaining = pageSize;
      this.local = local;
    }

    @Override
    protected ReadResult computeNext() {
      // DON'T rely on iterator.hasNext(), that
      // iterator only stops when the result set is fully fetched,
      // and triggers blocking background requests for the next pages.
      // DON'T rely either on rs.getAvailableWithoutFetching(),
      // it reports the total number of rows available in all pages,
      // including subsequent ones that were preemptively fetched, and
      // usually reports too many rows.
      if (remaining-- > 0) {
        Row row = rs.one();
        // row will be null if this is the last page and the actual page size
        // is less than pageSize
        if (row != null) {
          if (listener != null) {
            listener.onRowReceived(row, local);
          }
          return new DefaultReadResult(statement, rs.getExecutionInfo(), row);
        }
      }
      return endOfData();
    }
  }
}
