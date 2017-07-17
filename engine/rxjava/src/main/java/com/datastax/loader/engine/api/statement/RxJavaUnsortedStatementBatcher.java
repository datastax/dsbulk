/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.api.statement;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import io.reactivex.Flowable;
import io.reactivex.FlowableTransformer;

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

  private final int bufferSize;

  public RxJavaUnsortedStatementBatcher() {
    this(DEFAULT_BUFFER_SIZE);
  }

  public RxJavaUnsortedStatementBatcher(int bufferSize) {
    this.bufferSize = bufferSize;
  }

  public RxJavaUnsortedStatementBatcher(Cluster cluster) {
    this(cluster, DEFAULT_BUFFER_SIZE);
  }

  public RxJavaUnsortedStatementBatcher(Cluster cluster, int bufferSize) {
    super(cluster);
    this.bufferSize = bufferSize;
  }

  public RxJavaUnsortedStatementBatcher(
      BatchStatement.Type batchType, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    this(batchType, protocolVersion, codecRegistry, DEFAULT_BUFFER_SIZE);
  }

  public RxJavaUnsortedStatementBatcher(
      BatchStatement.Type batchType,
      ProtocolVersion protocolVersion,
      CodecRegistry codecRegistry,
      int bufferSize) {
    super(batchType, protocolVersion, codecRegistry);
    this.bufferSize = bufferSize;
  }

  @Override
  public Flowable<Statement> apply(Flowable<Statement> upstream) {
    return upstream.buffer(bufferSize).map(this::batchByRoutingKey).flatMap(Flowable::fromIterable);
  }
}
