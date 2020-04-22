/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.executor.api.subscription;

import com.datastax.dse.driver.api.core.cql.continuous.ContinuousAsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.shaded.guava.common.collect.AbstractIterator;
import com.datastax.oss.driver.shaded.guava.common.util.concurrent.RateLimiter;
import com.datastax.oss.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.oss.dsbulk.executor.api.listener.ExecutionContext;
import com.datastax.oss.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.oss.dsbulk.executor.api.result.DefaultReadResult;
import com.datastax.oss.dsbulk.executor.api.result.ReadResult;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Iterator;
import java.util.concurrent.Semaphore;
import org.reactivestreams.Subscriber;

public class ContinuousReadResultSubscription
    extends ResultSubscription<ReadResult, ContinuousAsyncResultSet> {

  public ContinuousReadResultSubscription(
      @NonNull Subscriber<? super ReadResult> subscriber,
      @NonNull Statement<?> statement,
      @Nullable ExecutionListener listener,
      @Nullable Semaphore maxConcurrentRequests,
      @Nullable Semaphore maxConcurrentQueries,
      @Nullable RateLimiter rateLimiter,
      boolean failFast) {
    super(
        subscriber,
        statement,
        listener,
        maxConcurrentRequests,
        maxConcurrentQueries,
        rateLimiter,
        failFast);
  }

  @Override
  Page toPage(ContinuousAsyncResultSet rs, ExecutionContext local) {
    Iterator<Row> rows = rs.currentPage().iterator();
    Iterator<ReadResult> results =
        new AbstractIterator<ReadResult>() {

          @Override
          protected ReadResult computeNext() {
            if (rows.hasNext()) {
              Row row = rows.next();
              if (listener != null) {
                listener.onRowReceived(row, local);
              }
              return new DefaultReadResult(statement, rs.getExecutionInfo(), row);
            }
            return endOfData();
          }
        };
    return new ContinuousPage(rs, results);
  }

  @Override
  public void cancel() {
    Page current = pages.peek();
    if (current instanceof ContinuousPage) {
      // forcibly cancel the continuous paging request
      ((ContinuousPage) current).rs.cancel();
    }
    super.cancel();
  }

  @Override
  void onRequestStarted(ExecutionContext local) {
    if (listener != null) {
      listener.onReadRequestStarted(statement, local);
    }
  }

  @Override
  void onRequestSuccessful(ContinuousAsyncResultSet page, ExecutionContext local) {
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
  void onBeforeResultEmitted(ReadResult result) {
    if (rateLimiter != null) {
      rateLimiter.acquire();
    }
  }

  @Override
  protected ReadResult toErrorResult(BulkExecutionException error) {
    return new DefaultReadResult(error);
  }

  private class ContinuousPage extends Page {

    final ContinuousAsyncResultSet rs;

    private ContinuousPage(ContinuousAsyncResultSet rs, Iterator<ReadResult> rows) {
      super(rows, rs.hasMorePages() ? rs::fetchNextPage : null);
      this.rs = rs;
    }
  }
}
