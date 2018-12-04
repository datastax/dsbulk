/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
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
        StatementBatcher.DEFAULT_MAX_BATCH_STATEMENTS,
        bufferSize,
        StatementBatcher.DEFAULT_MAX_SIZE_BYTES);
  }

  public ReactorUnsortedStatementBatcher(
      Cluster cluster,
      StatementBatcher.BatchMode batchMode,
      int maxBatchStatements,
      int bufferSize,
      long maxSizeInBytes) {
    this(
        cluster,
        batchMode,
        BatchStatement.Type.UNLOGGED,
        maxBatchStatements,
        bufferSize,
        maxSizeInBytes);
  }

  public ReactorUnsortedStatementBatcher(
      Cluster cluster,
      StatementBatcher.BatchMode batchMode,
      BatchStatement.Type batchType,
      int maxBatchStatements,
      int bufferSize,
      long maxSizeInBytes) {
    super(cluster, batchMode, batchType, maxBatchStatements, maxSizeInBytes);
    this.bufferSize = bufferSize;
  }

  @Override
  public Flux<Statement> apply(Flux<? extends Statement> upstream) {
    return upstream.window(bufferSize).flatMap(this::batchByGroupingKey);
  }
}
