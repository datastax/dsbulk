/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api;

import com.datastax.dse.driver.api.core.cql.continuous.ContinuousAsyncResultSet;
import com.datastax.dse.driver.api.core.cql.continuous.ContinuousSession;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metrics.Metrics;
import com.datastax.oss.driver.api.core.session.Request;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Optional;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CompletionStage;

/**
 * A {@link CqlSession} implementation that delegates to another session and ensures that futures
 * produced by it are completed in the order they were submitted.
 */
public class SerializedSession implements CqlSession, ContinuousSession {

  private final CqlSession session;

  // Guarded by this
  private CompletableFuture<Void> previous = CompletableFuture.completedFuture(null);

  public SerializedSession(CqlSession session) {
    this.session = session;
  }

  // Methods whose futures are serialized, i.e., chained together

  @NonNull
  @Override
  public CompletionStage<AsyncResultSet> executeAsync(@NonNull Statement statement) {
    return chain(session.executeAsync(statement));
  }

  @NonNull
  @Override
  public CompletionStage<ContinuousAsyncResultSet> executeContinuouslyAsync(
      @NonNull Statement<?> statement) {
    return chain(((ContinuousSession) session).executeContinuouslyAsync(statement));
  }

  @SuppressWarnings("FutureReturnValueIgnored")
  private synchronized <T> CompletionStage<T> chain(CompletionStage<T> current) {
    CompletableFuture<T> next = new CompletableFuture<>();
    CompletableFuture.allOf(previous, current.toCompletableFuture())
        .whenComplete(
            (rs, error) -> {
              try {
                next.complete(current.toCompletableFuture().join());
              } catch (CompletionException | CancellationException e) {
                next.completeExceptionally(e.getCause());
              }
            });
    previous = next.thenApply(rs -> null);
    return next;
  }

  // Other methods (not chained)

  @Override
  public <RequestT extends Request, ResultT> ResultT execute(
      @NonNull RequestT request, @NonNull GenericType<ResultT> resultType) {
    return session.execute(request, resultType);
  }

  @NonNull
  @Override
  public String getName() {
    return session.getName();
  }

  @NonNull
  @Override
  public Metadata getMetadata() {
    return session.getMetadata();
  }

  @Override
  public boolean isSchemaMetadataEnabled() {
    return session.isSchemaMetadataEnabled();
  }

  @NonNull
  @Override
  public CompletionStage<Metadata> setSchemaMetadataEnabled(Boolean newValue) {
    return session.setSchemaMetadataEnabled(newValue);
  }

  @NonNull
  @Override
  public CompletionStage<Metadata> refreshSchemaAsync() {
    return session.refreshSchemaAsync();
  }

  @NonNull
  @Override
  public CompletionStage<Boolean> checkSchemaAgreementAsync() {
    return session.checkSchemaAgreementAsync();
  }

  @NonNull
  @Override
  public DriverContext getContext() {
    return session.getContext();
  }

  @NonNull
  @Override
  public Optional<CqlIdentifier> getKeyspace() {
    return session.getKeyspace();
  }

  @NonNull
  @Override
  public Optional<Metrics> getMetrics() {
    return session.getMetrics();
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeFuture() {
    return session.closeFuture();
  }

  @NonNull
  @Override
  public CompletionStage<Void> forceCloseAsync() {
    return session.forceCloseAsync();
  }

  @NonNull
  @Override
  public CompletionStage<Void> closeAsync() {
    return session.closeAsync();
  }
}
