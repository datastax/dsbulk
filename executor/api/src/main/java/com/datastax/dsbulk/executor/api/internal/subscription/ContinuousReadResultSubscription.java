/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.internal.subscription;

import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.internal.result.DefaultReadResult;
import com.datastax.dsbulk.executor.api.listener.ExecutionContext;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.datastax.dse.driver.api.core.cql.continuous.ContinuousAsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import org.reactivestreams.Subscriber;

@SuppressWarnings("OptionalUsedAsFieldOrParameterType")
public class ContinuousReadResultSubscription
    extends ResultSubscription<ReadResult, ContinuousAsyncResultSet> {

  public ContinuousReadResultSubscription(
      Subscriber<? super ReadResult> subscriber,
      Statement statement,
      Optional<ExecutionListener> listener,
      Optional<Semaphore> requestPermits,
      Optional<RateLimiter> rateLimiter,
      boolean failFast) {
    super(subscriber, statement, listener, requestPermits, rateLimiter, failFast);
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
              listener.ifPresent(l -> l.onRowReceived(row, local));
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
  protected ReadResult toErrorResult(BulkExecutionException error) {
    return new DefaultReadResult(error);
  }

  @Override
  void onRequestStarted(ExecutionContext local) {
    listener.ifPresent(l -> l.onReadRequestStarted(statement, local));
  }

  @Override
  void onRequestSuccessful(ContinuousAsyncResultSet page, ExecutionContext local) {
    listener.ifPresent(l -> l.onReadRequestSuccessful(statement, local));
  }

  @Override
  void onRequestFailed(Throwable t, ExecutionContext local) {
    listener.ifPresent(l -> l.onReadRequestFailed(statement, t, local));
  }

  private class ContinuousPage extends Page {

    final ContinuousAsyncResultSet rs;

    private ContinuousPage(ContinuousAsyncResultSet rs, Iterator<ReadResult> rows) {
      super(rows, rs.hasMorePages() ? rs::fetchNextPage : null);
      this.rs = rs;
    }
  }
}
