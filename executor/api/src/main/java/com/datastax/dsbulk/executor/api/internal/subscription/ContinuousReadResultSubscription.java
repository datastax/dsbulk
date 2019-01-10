/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.internal.subscription;

import com.datastax.driver.core.AsyncContinuousPagingResult;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.exception.BulkExecutionException;
import com.datastax.dsbulk.executor.api.internal.result.DefaultReadResult;
import com.datastax.dsbulk.executor.api.listener.ExecutionContext;
import com.datastax.dsbulk.executor.api.listener.ExecutionListener;
import com.datastax.dsbulk.executor.api.result.ReadResult;
import com.google.common.collect.AbstractIterator;
import com.google.common.util.concurrent.RateLimiter;
import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.Semaphore;
import org.reactivestreams.Subscriber;

@SuppressWarnings({"OptionalUsedAsFieldOrParameterType", "UnstableApiUsage"})
public class ContinuousReadResultSubscription
    extends ResultSubscription<ReadResult, AsyncContinuousPagingResult> {

  public ContinuousReadResultSubscription(
      Subscriber<? super ReadResult> subscriber,
      Statement statement,
      Optional<ExecutionListener> listener,
      Optional<Semaphore> maxConcurrentRequests,
      Optional<Semaphore> maxConcurrentQueries,
      Optional<RateLimiter> rateLimiter,
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
  Page toPage(AsyncContinuousPagingResult rs, ExecutionContext local) {
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
  void onRequestStarted(ExecutionContext local) {
    listener.ifPresent(l -> l.onReadRequestStarted(statement, local));
  }

  @Override
  void onRequestSuccessful(AsyncContinuousPagingResult page, ExecutionContext local) {
    listener.ifPresent(l -> l.onReadRequestSuccessful(statement, local));
  }

  @Override
  void onRequestFailed(Throwable t, ExecutionContext local) {
    listener.ifPresent(l -> l.onReadRequestFailed(statement, t, local));
  }

  @Override
  void onBeforeResultEmitted(ReadResult result) {
    rateLimiter.ifPresent(RateLimiter::acquire);
  }

  @Override
  boolean isLastPage(AsyncContinuousPagingResult page) {
    return page.isLast();
  }

  @Override
  protected ReadResult toErrorResult(BulkExecutionException error) {
    return new DefaultReadResult(error);
  }

  private class ContinuousPage extends Page {

    final AsyncContinuousPagingResult rs;

    private ContinuousPage(AsyncContinuousPagingResult rs, Iterator<ReadResult> rows) {
      super(rows, rs.isLast() ? null : rs::nextPage);
      this.rs = rs;
    }
  }
}
