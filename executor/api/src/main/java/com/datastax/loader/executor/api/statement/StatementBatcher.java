/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.executor.api.statement;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.Token;
import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A helper class to facilitate grouping statements together in batches sharing the same routing
 * key.
 *
 * @see <a href="http://docs.datastax.com/en/cql/3.1/cql/cql_using/useBatch.html">Using and misusing
 *     batches</a>
 */
public class StatementBatcher {

  protected final BatchStatement.Type batchType;
  protected final ProtocolVersion protocolVersion;
  protected final CodecRegistry codecRegistry;

  /**
   * Creates a new {@link StatementBatcher} that produces {@link
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, and uses the {@link
   * ProtocolVersion#NEWEST_SUPPORTED latest stable} protocol version and the default {@link
   * CodecRegistry#DEFAULT_INSTANCE CodecRegistry} instance.
   */
  public StatementBatcher() {
    this(
        BatchStatement.Type.UNLOGGED,
        ProtocolVersion.NEWEST_SUPPORTED,
        CodecRegistry.DEFAULT_INSTANCE);
  }

  /**
   * Creates a new {@link StatementBatcher} that produces {@link
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, and uses the given
   * {@link Cluster} as its source for the {@link ProtocolVersion protocol version} and the {@link
   * CodecRegistry} instance to use.
   *
   * @param cluster The {@link Cluster} to use.
   */
  public StatementBatcher(Cluster cluster) {
    this(
        BatchStatement.Type.UNLOGGED,
        cluster.getConfiguration().getProtocolOptions().getProtocolVersion(),
        cluster.getConfiguration().getCodecRegistry());
  }

  /**
   * Creates a new {@link StatementBatcher} that produces batches of the given {@code batchType},
   * and uses the given {@code protocolVersion} and the given {@code codecRegistry}.
   *
   * @param batchType The {@link com.datastax.driver.core.BatchStatement.Type batch type} to use.
   * @param protocolVersion The {@link ProtocolVersion} to use.
   * @param codecRegistry The {@link CodecRegistry} to use.
   */
  public StatementBatcher(
      BatchStatement.Type batchType, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    this.batchType = batchType;
    this.protocolVersion = protocolVersion;
    this.codecRegistry = codecRegistry;
  }

  /**
   * Batches together the given statements into groups of statements having the same {@link
   * Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key}.
   *
   * @param statements the statements to batch together.
   * @return A list of batched statements.
   */
  public List<Statement> batchByRoutingKey(Statement... statements) {
    return batchByRoutingKey(Arrays.asList(statements));
  }

  /**
   * Batches together the given statements into groups of statements having the same {@link
   * Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key}.
   *
   * @param statements the statements to batch together.
   * @return A list of batched statements.
   */
  public List<Statement> batchByRoutingKey(Iterable<? extends Statement> statements) {
    return StreamSupport.stream(statements.spliterator(), false)
        .collect(Collectors.groupingBy(this::routingKey))
        .values()
        .stream()
        .map(this::maybeBatch)
        .collect(Collectors.toList());
  }

  /**
   * Batches together all the given statements into one single {@link BatchStatement}.
   *
   * <p>Use this method with caution; if the given statements do not share the same {@link
   * Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key}, the resulting batch could
   * lead to write throughput degradation.
   *
   * @param statements the statements to batch together.
   * @return A publisher of one single {@link BatchStatement} containing all the given statements
   *     batched together.
   */
  public Statement batchSingle(Statement... statements) {
    return batchSingle(Arrays.asList(statements));
  }

  /**
   * Batches together all the given statements into one single {@link BatchStatement}.
   *
   * <p>Use this method with caution; if the given statements do not share the same {@link
   * Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key}, the resulting batch could
   * lead to write throughput degradation.
   *
   * @param statements the statements to batch together.
   * @return A publisher of one single {@link BatchStatement} containing all the given statements
   *     batched together.
   */
  public Statement batchSingle(Collection<? extends Statement> statements) {
    return maybeBatch(statements);
  }

  private Statement maybeBatch(Collection<? extends Statement> stmts) {
    Objects.requireNonNull(stmts);
    Preconditions.checkArgument(!stmts.isEmpty());
    // Don't wrap single statements in batch.
    if (stmts.size() == 1) {
      return stmts.iterator().next();
    } else {
      BatchStatement batch = new BatchStatement(batchType);
      stmts.forEach(batch::add);
      return batch;
    }
  }

  Object routingKey(Statement statement) {
    ByteBuffer routingKey = statement.getRoutingKey(protocolVersion, codecRegistry);
    if (routingKey != null) {
      return routingKey;
    }
    Token routingToken = statement.getRoutingToken();
    if (routingToken != null) {
      return routingToken;
    }
    // If no routing key or token can be discerned, return the statement itself so it
    // will stay unbatched.
    return statement;
  }
}
