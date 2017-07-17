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
import io.reactivex.functions.Predicate;

/**
 * An {@link RxJavaStatementBatcher} that implements {@link FlowableTransformer} so that it can be
 * used in a {@link Flowable#compose(FlowableTransformer) compose} operation to batch statements
 * together.
 *
 * <p>This operator assumes that the upstream source delivers statements whose partition keys are
 * already grouped together; when a new partition key is detected, a batch is created with the
 * accumulated items and passed downstream.
 *
 * @see RxJavaStatementBatcher
 * @see RxJavaUnsortedStatementBatcher
 */
public class RxJavaSortedStatementBatcher extends RxJavaStatementBatcher
    implements FlowableTransformer<Statement, Statement> {

  /** The default maximum size for a batch. */
  public static final int DEFAULT_MAX_BUFFER_SIZE = 1000;

  private final int maxBufferSize;

  public RxJavaSortedStatementBatcher() {
    this(DEFAULT_MAX_BUFFER_SIZE);
  }

  public RxJavaSortedStatementBatcher(int maxBufferSize) {
    this.maxBufferSize = maxBufferSize;
  }

  public RxJavaSortedStatementBatcher(Cluster cluster) {
    this(cluster, DEFAULT_MAX_BUFFER_SIZE);
  }

  public RxJavaSortedStatementBatcher(Cluster cluster, int maxBufferSize) {
    super(cluster);
    this.maxBufferSize = maxBufferSize;
  }

  public RxJavaSortedStatementBatcher(
      BatchStatement.Type batchType, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    this(batchType, protocolVersion, codecRegistry, DEFAULT_MAX_BUFFER_SIZE);
  }

  public RxJavaSortedStatementBatcher(
      BatchStatement.Type batchType,
      ProtocolVersion protocolVersion,
      CodecRegistry codecRegistry,
      int maxBufferSize) {
    super(batchType, protocolVersion, codecRegistry);
    this.maxBufferSize = maxBufferSize;
  }

  @Override
  public Flowable<Statement> apply(Flowable<Statement> upstream) {
    Flowable<Statement> connectableFlowable = upstream.publish().autoConnect(2);
    Flowable<Statement> boundarySelector =
        connectableFlowable.filter(new StatementBatcherPredicate());
    return connectableFlowable.buffer(boundarySelector).map(this::batchSingle);
  }

  private class StatementBatcherPredicate implements Predicate<Statement> {

    private Object currentRoutingKey;

    private int size = 0;

    @Override
    public boolean test(Statement statement) {
      boolean bufferFull = ++size > maxBufferSize;
      Object routingKey = routingKey(statement);
      boolean routingKeyChanged = currentRoutingKey != null && currentRoutingKey != routingKey;
      this.currentRoutingKey = routingKey;
      boolean shouldFlush = routingKeyChanged || bufferFull;
      if (shouldFlush) size = 0;
      return shouldFlush;
    }
  }
}
