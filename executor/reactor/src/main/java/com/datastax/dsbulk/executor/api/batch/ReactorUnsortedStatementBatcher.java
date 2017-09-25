/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.batch;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Statement;
import java.time.Duration;
import java.util.function.Function;
import reactor.core.publisher.Flux;

/**
 * An {@link ReactorStatementBatcher} that implements {@code Function<Flux<Statement>,
 * Flux<Statement>>} so that it can be used in a {@link Flux#compose(Function) compose} operation to
 * batch statements together.
 *
 * <p>This operator assumes that the upstream source delivers statements whose partition keys are
 * randomly distributed; when the internal buffer is full, batches are created with the accumulated
 * items and passed downstream.
 *
 * @see ReactorStatementBatcher
 * @see ReactorSortedStatementBatcher
 */
public class ReactorUnsortedStatementBatcher extends ReactorStatementBatcher
    implements Function<Flux<? extends Statement>, Flux<Statement>> {

  /** The default size for the internal buffer. */
  public static final int DEFAULT_BUFFER_SIZE = 1000;

  public static final Duration DEFAULT_BUFFER_TIMEOUT = Duration.ofSeconds(5);

  private final int bufferSize;
  private final Duration bufferTimeout;

  public ReactorUnsortedStatementBatcher() {
    this(DEFAULT_BUFFER_SIZE, DEFAULT_BUFFER_TIMEOUT);
  }

  public ReactorUnsortedStatementBatcher(int bufferSize, Duration bufferTimeout) {
    this.bufferSize = bufferSize;
    this.bufferTimeout = bufferTimeout;
  }

  public ReactorUnsortedStatementBatcher(Cluster cluster) {
    this(cluster, DEFAULT_BUFFER_SIZE, DEFAULT_BUFFER_TIMEOUT);
  }

  public ReactorUnsortedStatementBatcher(Cluster cluster, int bufferSize, Duration bufferTimeout) {
    this(cluster, BatchMode.PARTITION_KEY, DEFAULT_MAX_BATCH_SIZE, bufferSize, bufferTimeout);
  }

  public ReactorUnsortedStatementBatcher(
      Cluster cluster,
      BatchMode batchMode,
      int maxBatchSize,
      int bufferSize,
      Duration bufferTimeout) {
    this(cluster, batchMode, BatchStatement.Type.UNLOGGED, maxBatchSize, bufferSize, bufferTimeout);
  }

  public ReactorUnsortedStatementBatcher(
      Cluster cluster,
      BatchMode batchMode,
      BatchStatement.Type batchType,
      int maxBatchSize,
      int bufferSize,
      Duration bufferTimeout) {
    super(cluster, batchMode, batchType, maxBatchSize);
    this.bufferSize = bufferSize;
    this.bufferTimeout = bufferTimeout;
  }

  @Override
  public Flux<Statement> apply(Flux<? extends Statement> upstream) {
    return upstream
        .bufferTimeout(bufferSize, bufferTimeout)
        .map(this::batchByGroupingKey)
        .flatMap(Flux::fromIterable);
  }
}
