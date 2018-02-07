/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.driver.core;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;

public class SerializedSession extends AbstractSession {

  private final AbstractSession session;

  private final AtomicReference<ResultSetFuture> resultSetFutureRef = new AtomicReference<>();
  private final AtomicReference<ListenableFuture<AsyncContinuousPagingResult>> continuousResultRef =
      new AtomicReference<>();

  public SerializedSession(Session session) {
    this.session = (AbstractSession) session;
  }

  @Override
  public ResultSetFuture executeAsync(String query) {
    return chainFutures(session.executeAsync(query));
  }

  @Override
  public ResultSetFuture executeAsync(String query, Map<String, Object> values) {
    return chainFutures(session.executeAsync(query, values));
  }

  @Override
  public ResultSetFuture executeAsync(String query, Object... values) {
    return chainFutures(session.executeAsync(query, values));
  }

  @Override
  public ResultSetFuture executeAsync(Statement statement) {
    return chainFutures(session.executeAsync(statement));
  }

  private ResultSetFuture chainFutures(ResultSetFuture current) {
    for (; ; ) {
      ResultSetFuture previous = resultSetFutureRef.get();
      SerializedResultSetFuture next = new SerializedResultSetFuture(previous, current);
      if (resultSetFutureRef.compareAndSet(previous, next)) {
        next.init();
        return next;
      }
    }
  }

  @Override
  public ListenableFuture<AsyncContinuousPagingResult> executeContinuouslyAsync(
      Statement statement, ContinuousPagingOptions options) {
    ListenableFuture<AsyncContinuousPagingResult> current =
        session.executeContinuouslyAsync(statement, options);
    for (; ; ) {
      ListenableFuture<AsyncContinuousPagingResult> previous = continuousResultRef.get();
      SettableFuture<AsyncContinuousPagingResult> next = SettableFuture.create();
      if (continuousResultRef.compareAndSet(previous, next)) {
        if (previous == null) {
          Futures.addCallback(
              current,
              new FutureCallback<AsyncContinuousPagingResult>() {
                @Override
                public void onSuccess(AsyncContinuousPagingResult result) {
                  next.set(result);
                }

                @Override
                public void onFailure(Throwable t) {
                  next.setException(t);
                }
              },
              MoreExecutors.directExecutor());
        } else {
          Futures.whenAllComplete(previous, current)
              .call(
                  () -> {
                    AsyncContinuousPagingResult result = null;
                    try {
                      result = current.get();
                      next.set(result);
                    } catch (ExecutionException e) {
                      next.setException(e.getCause());
                    }
                    return result;
                  },
                  MoreExecutors.directExecutor());
        }
        return next;
      }
    }
  }

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
  public PreparedStatement prepare(String query) {
    return session.prepare(query);
  }

  @Override
  public PreparedStatement prepare(RegularStatement statement) {
    return session.prepare(statement);
  }

  @Override
  public ListenableFuture<PreparedStatement> prepareAsync(String query) {
    return session.prepareAsync(query);
  }

  @Override
  public ListenableFuture<PreparedStatement> prepareAsync(RegularStatement statement) {
    return session.prepareAsync(statement);
  }

  @Override
  public ListenableFuture<PreparedStatement> prepareAsync(
      String query, String keyspace, Map<String, ByteBuffer> customPayload) {
    return session.prepareAsync(query, keyspace, customPayload);
  }

  @Override
  public void close() {
    session.close();
  }

  @Override
  public Cluster getConcreteCluster() {
    return session.getConcreteCluster();
  }

  @Override
  public void checkNotInEventLoop() {
    session.checkNotInEventLoop();
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

  @Override
  public ContinuousPagingResult executeContinuously(
      Statement statement, ContinuousPagingOptions options) {
    return session.executeContinuously(statement, options);
  }
}
