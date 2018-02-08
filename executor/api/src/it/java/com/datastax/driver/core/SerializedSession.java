/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.driver.core;

import com.datastax.driver.core.utils.MoreFutures;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * A {@link Session} implementation that delegates to another session and ensures that futures
 * produced by it are completed in the order they were submitted.
 */
public class SerializedSession implements Session, ContinuousPagingSession {

  private final Session session;

  // Guarded by this
  private ListenableFuture<Void> previous = MoreFutures.VOID_SUCCESS;

  public SerializedSession(Session session) {
    this.session = session;
  }

  // Methods whose futures are serialized, i.e., chained together

  @Override
  public ResultSetFuture executeAsync(String query) {
    return new DelegatingResultSetFuture(chain(session.executeAsync(query)));
  }

  @Override
  public ResultSetFuture executeAsync(String query, Map<String, Object> values) {
    return new DelegatingResultSetFuture(chain(session.executeAsync(query, values)));
  }

  @Override
  public ResultSetFuture executeAsync(String query, Object... values) {
    return new DelegatingResultSetFuture(chain(session.executeAsync(query, values)));
  }

  @Override
  public ResultSetFuture executeAsync(Statement statement) {
    return new DelegatingResultSetFuture(chain(session.executeAsync(statement)));
  }

  @Override
  public ListenableFuture<AsyncContinuousPagingResult> executeContinuouslyAsync(
      Statement statement, ContinuousPagingOptions options) {
    return chain(((ContinuousPagingSession) session).executeContinuouslyAsync(statement, options));
  }

  private synchronized <T> SettableFuture<T> chain(ListenableFuture<T> current) {
    SettableFuture<T> next = SettableFuture.create();
    Futures.whenAllComplete(previous, current)
        .call(
            () -> {
              try {
                next.set(current.get());
              } catch (ExecutionException e) {
                next.setException(e.getCause());
              }
              return null;
            },
            MoreExecutors.directExecutor());
    previous = Futures.transform(next, rs -> null, MoreExecutors.directExecutor());
    return next;
  }

  // Other methods (not chained)

  @Override
  public ResultSet execute(String query) {
    return session.execute(query);
  }

  @Override
  public ResultSet execute(String query, Object... values) {
    return session.execute(query, values);
  }

  @Override
  public ResultSet execute(String query, Map<String, Object> values) {
    return session.execute(query, values);
  }

  @Override
  public ResultSet execute(Statement statement) {
    return session.execute(statement);
  }

  @Override
  public ContinuousPagingResult executeContinuously(
      Statement statement, ContinuousPagingOptions options) {
    return ((ContinuousPagingSession) session).executeContinuously(statement, options);
  }

  @Override
  public PreparedStatement prepare(String query) {
    return session.prepare(query);
  }

  @Override
  public PreparedStatement prepare(RegularStatement statement) {
    return session.prepare(statement);
  }

  @Override
  public ListenableFuture<PreparedStatement> prepareAsync(String query) {
    return chain(session.prepareAsync(query));
  }

  @Override
  public ListenableFuture<PreparedStatement> prepareAsync(RegularStatement statement) {
    return chain(session.prepareAsync(statement));
  }

  @Override
  public String getLoggedKeyspace() {
    return session.getLoggedKeyspace();
  }

  @Override
  public Session init() {
    return session.init();
  }

  @Override
  public ListenableFuture<Session> initAsync() {
    return session.initAsync();
  }

  @Override
  public void close() {
    session.close();
  }

  @Override
  public CloseFuture closeAsync() {
    return session.closeAsync();
  }

  @Override
  public boolean isClosed() {
    return session.isClosed();
  }

  @Override
  public Cluster getCluster() {
    return session.getCluster();
  }

  @Override
  public State getState() {
    return session.getState();
  }
}
