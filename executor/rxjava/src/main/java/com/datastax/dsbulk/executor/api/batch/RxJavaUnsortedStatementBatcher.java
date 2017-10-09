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
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * An {@link RxJavaStatementBatcher} that implements {@link FlowableTransformer} so that it can be
 * used in a {@link Flowable#compose(FlowableTransformer) compose} operation to batch statements
 * together.
 *
 * <p>This operator assumes that the upstream source delivers statements whose partition keys are
 * randomly distributed; when the internal buffer is full, batches are created with the accumulated
 * items and passed downstream.
 *
 * @see RxJavaStatementBatcher
 * @see RxJavaSortedStatementBatcher
 */
public class RxJavaUnsortedStatementBatcher extends RxJavaStatementBatcher
    implements FlowableTransformer<Statement, Statement> {

  /** The default size for the internal buffer. */
  public static final int DEFAULT_BUFFER_SIZE = 1000;

  public static final Duration DEFAULT_BufferTimeout = Duration.ofSeconds(5);

  private final int bufferSize;
  private final Duration bufferTimeout;

  public RxJavaUnsortedStatementBatcher() {
    this(DEFAULT_BUFFER_SIZE, DEFAULT_BufferTimeout);
  }

  public RxJavaUnsortedStatementBatcher(int bufferSize, Duration bufferTimeout) {
    this.bufferSize = bufferSize;
    this.bufferTimeout = bufferTimeout;
  }

  public RxJavaUnsortedStatementBatcher(Cluster cluster) {
    this(cluster, DEFAULT_BUFFER_SIZE, DEFAULT_BufferTimeout);
  }

  public RxJavaUnsortedStatementBatcher(Cluster cluster, int bufferSize, Duration bufferTimeout) {
    this(cluster, BatchMode.PARTITION_KEY, DEFAULT_MAX_BATCH_SIZE, bufferSize, bufferTimeout);
  }

  public RxJavaUnsortedStatementBatcher(
      Cluster cluster,
      BatchMode batchMode,
      int maxBatchSize,
      int bufferSize,
      Duration bufferTimeout) {
    this(cluster, batchMode, BatchStatement.Type.UNLOGGED, maxBatchSize, bufferSize, bufferTimeout);
  }

  public RxJavaUnsortedStatementBatcher(
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
  public Flowable<Statement> apply(Flowable<Statement> upstream) {
    return upstream
        .buffer(bufferTimeout.getSeconds(), TimeUnit.SECONDS, bufferSize)
        .map(this::batchByGroupingKey)
        .flatMap(Flowable::fromIterable);
  }
}
