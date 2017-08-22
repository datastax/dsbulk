/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api.batch;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import java.util.function.Function;
import java.util.function.Predicate;
import reactor.core.publisher.Flux;

/**
 * An {@link ReactorStatementBatcher} that implements {@code Function<Flux<Statement>,
 * Flux<Statement>>} so that it can be used in a {@link Flux#compose(Function) compose} operation to
 * batch statements together.
 *
 * <p>This operator assumes that the upstream source delivers statements whose partition keys are
 * already grouped together; when a new partition key is detected, a batch is created with the
 * accumulated items and passed downstream.
 *
 * <p>Use this operator with caution; if the given statements do not have their {@link
 * Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key} already grouped together,
 * the resulting batch could lead to sub-optimal write performance.
 *
 * @see ReactorStatementBatcher
 * @see ReactorUnsortedStatementBatcher
 */
public class ReactorSortedStatementBatcher extends ReactorStatementBatcher
    implements Function<Flux<? extends Statement>, Flux<Statement>> {

  /** The default maximum size for a batch. */
  public static final int DEFAULT_MAX_BUFFER_SIZE = 1000;

  private final int maxBufferSize;

  public ReactorSortedStatementBatcher() {
    this(DEFAULT_MAX_BUFFER_SIZE);
  }

  public ReactorSortedStatementBatcher(int maxBufferSize) {
    this.maxBufferSize = maxBufferSize;
  }

  public ReactorSortedStatementBatcher(Cluster cluster) {
    this(cluster, DEFAULT_MAX_BUFFER_SIZE);
  }

  public ReactorSortedStatementBatcher(Cluster cluster, int maxBufferSize) {
    this(cluster, StatementBatcher.BatchMode.PARTITION_KEY, maxBufferSize);
  }

  public ReactorSortedStatementBatcher(
      Cluster cluster, StatementBatcher.BatchMode batchMode, int maxBufferSize) {
    this(cluster, batchMode, BatchStatement.Type.UNLOGGED, maxBufferSize);
  }

  public ReactorSortedStatementBatcher(
      Cluster cluster,
      StatementBatcher.BatchMode batchMode,
      BatchStatement.Type batchType,
      int maxBufferSize) {
    super(cluster, batchMode, batchType);
    this.maxBufferSize = maxBufferSize;
  }

  @Override
  public Flux<Statement> apply(Flux<? extends Statement> upstream) {
    Flux<? extends Statement> connectableFlux = upstream.publish().autoConnect(2);
    Flux<? extends Statement> boundarySelector =
        connectableFlux.filter(new StatementBatcherPredicate());
    return connectableFlux.buffer(boundarySelector).map(this::batchAll);
  }

  private class StatementBatcherPredicate implements Predicate<Statement> {

    private Object groupingKey;

    private int size = 0;

    @Override
    public boolean test(Statement statement) {
      boolean bufferFull = ++size > maxBufferSize;
      Object groupingKey = groupingKey(statement);
      boolean groupingKeyChanged =
          this.groupingKey != null && !this.groupingKey.equals(groupingKey);
      this.groupingKey = groupingKey;
      boolean shouldFlush = groupingKeyChanged || bufferFull;
      if (shouldFlush) {
        size = 0;
      }
      return shouldFlush;
    }
  }
}
