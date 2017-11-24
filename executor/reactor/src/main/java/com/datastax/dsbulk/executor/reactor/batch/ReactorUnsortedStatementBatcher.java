/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.reactor.batch;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Statement;
import com.datastax.dsbulk.executor.api.batch.StatementBatcher;
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
  public static final int DEFAULT_BUFFER_SIZE = 512;

  private final int bufferSize;

  public ReactorUnsortedStatementBatcher() {
    this(DEFAULT_BUFFER_SIZE);
  }

  public ReactorUnsortedStatementBatcher(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  public ReactorUnsortedStatementBatcher(Cluster cluster) {
    this(cluster, DEFAULT_BUFFER_SIZE);
  }

  public ReactorUnsortedStatementBatcher(Cluster cluster, int bufferSize) {
    this(
        cluster,
        StatementBatcher.BatchMode.PARTITION_KEY,
        StatementBatcher.DEFAULT_MAX_BATCH_SIZE,
        bufferSize);
  }

  public ReactorUnsortedStatementBatcher(
      Cluster cluster, StatementBatcher.BatchMode batchMode, int maxBatchSize, int bufferSize) {
    this(cluster, batchMode, BatchStatement.Type.UNLOGGED, maxBatchSize, bufferSize);
  }

  public ReactorUnsortedStatementBatcher(
      Cluster cluster,
      StatementBatcher.BatchMode batchMode,
      BatchStatement.Type batchType,
      int maxBatchSize,
      int bufferSize) {
    super(cluster, batchMode, batchType, maxBatchSize);
    this.bufferSize = bufferSize;
  }

  @Override
  public Flux<Statement> apply(Flux<? extends Statement> upstream) {
    return upstream.window(bufferSize).flatMap(this::batchByGroupingKey);
  }
}
