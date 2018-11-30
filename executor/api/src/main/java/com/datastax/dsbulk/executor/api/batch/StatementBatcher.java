/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.batch;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.Token;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * A helper class to facilitate grouping statements together in batches sharing the same {@link
 * BatchMode#PARTITION_KEY partition key} or {@link BatchMode#REPLICA_SET replica set}.
 *
 * <p>Important: for this utility to work properly, statements must have their {@link
 * Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key} or their {@link
 * Statement#getRoutingToken() routing token} set. Furthermore, in {@link BatchMode#REPLICA_SET
 * replica set} it is also required that they have their {@link Statement#getKeyspace() keyspace}
 * set.
 *
 * @see <a href="http://docs.datastax.com/en/cql/3.1/cql/cql_using/useBatch.html">Using and misusing
 *     batches</a>
 */
public class StatementBatcher {

  /** Available batch modes. */
  public enum BatchMode {

    /**
     * Groups together statements that share the same partition key. This is the default mode, and
     * the preferred one.
     *
     * <p>Under the hood, this mode uses either the statement's {@link
     * Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key} or {@link
     * Statement#getRoutingToken() routing token}, whichever is available, starting with the routing
     * token.
     */
    PARTITION_KEY,

    /**
     * Groups together statements that share the same replica set. This mode might yield better
     * results for small clusters and lower replication factors, but tends to perform equally well
     * or even worse than {@link #PARTITION_KEY} for larger clusters or high replication factors
     * (i.e. RF &gt; 3).
     *
     * <p>Note that this mode can only work if the statements to batch have their {@link
     * Statement#getKeyspace() keypsace} set. If this condition is not met, the batcher will
     * silently fall back to {@code PARTITION_KEY} mode.
     */
    REPLICA_SET
  }


  public static final int DEFAULT_MAX_BATCH_SIZE = 100;
  public static final int DEFAULT_MAX_BATCH_STATEMENTS = 32;
  public static final long DEFAULT_MAX_SIZE_BYTES = -1;

  protected final Cluster cluster;
  protected final BatchMode batchMode;
  protected final BatchStatement.Type batchType;
  protected final ProtocolVersion protocolVersion;
  protected final CodecRegistry codecRegistry;
  @Deprecated
  protected final int maxBatchSize;
  protected final int maxBatchStatements;
  protected final long maxSizeInBytes;

  /**
   * Creates a new {@link StatementBatcher} that produces {@link
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, operates in {@link
   * BatchMode#PARTITION_KEY partition key} mode and uses the {@link
   * ProtocolVersion#NEWEST_SUPPORTED latest stable} protocol version and the default {@link
   * CodecRegistry#DEFAULT_INSTANCE CodecRegistry} instance. It also uses the default maximum batch
   * size (100).
   */
  public StatementBatcher() {
    this.cluster = null;
    this.batchMode = BatchMode.PARTITION_KEY;
    this.batchType = BatchStatement.Type.UNLOGGED;
    protocolVersion = ProtocolVersion.NEWEST_SUPPORTED;
    codecRegistry = CodecRegistry.DEFAULT_INSTANCE;
    maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
    maxBatchStatements = DEFAULT_MAX_BATCH_STATEMENTS;
    maxSizeInBytes = DEFAULT_MAX_SIZE_BYTES;
  }

  /**
   * Creates a new {@link StatementBatcher} that produces {@link
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, operates in {@link
   * BatchMode#PARTITION_KEY partition key} mode and uses the {@link
   * ProtocolVersion#NEWEST_SUPPORTED latest stable} protocol version and the default {@link
   * CodecRegistry#DEFAULT_INSTANCE CodecRegistry} instance. It uses the given maximum batch size.
   *
   * @param maxBatchSize The maximum batch size; must be &gt; 1.
   */
  //todo how to support new constructor with the same signature bot for maxBatchStatements?
  public StatementBatcher(int maxBatchSize) {
    this.cluster = null;
    this.batchMode = BatchMode.PARTITION_KEY;
    this.batchType = BatchStatement.Type.UNLOGGED;
    protocolVersion = ProtocolVersion.NEWEST_SUPPORTED;
    codecRegistry = CodecRegistry.DEFAULT_INSTANCE;
    this.maxBatchSize = maxBatchSize;
    maxBatchStatements = maxBatchSize;
    maxSizeInBytes = DEFAULT_MAX_SIZE_BYTES;
  }

  public StatementBatcher(long maxSizeInBytes) {
    this.cluster = null;
    this.batchMode = BatchMode.PARTITION_KEY;
    this.batchType = BatchStatement.Type.UNLOGGED;
    protocolVersion = ProtocolVersion.NEWEST_SUPPORTED;
    codecRegistry = CodecRegistry.DEFAULT_INSTANCE;
    maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
    maxBatchStatements = DEFAULT_MAX_BATCH_STATEMENTS;
    this.maxSizeInBytes = maxSizeInBytes;
  }


  /**
   * @param maxBatchStatements - the maximum number of statements in one batch. If set
   *                           to <= 0 it means that there is no limit of statements in one batch
   * @param maxSizeInBytes - the maximum number of bytes in one batch. If set to <= 0
   *                       it means that there is no size limit in one batch.
   */

  public StatementBatcher(int maxBatchStatements, long maxSizeInBytes) {
    this.cluster = null;
    this.batchMode = BatchMode.PARTITION_KEY;
    this.batchType = BatchStatement.Type.UNLOGGED;
    protocolVersion = ProtocolVersion.NEWEST_SUPPORTED;
    codecRegistry = CodecRegistry.DEFAULT_INSTANCE;
    maxBatchSize = DEFAULT_MAX_BATCH_SIZE;
    this.maxBatchStatements = maxBatchStatements;
    this.maxSizeInBytes = maxSizeInBytes;
  }

  /**
   * Creates a new {@link StatementBatcher} that produces {@link
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, operates in {@link
   * BatchMode#PARTITION_KEY partition key} mode and uses the given {@link Cluster} as its source
   * for the {@link ProtocolVersion protocol version} and the {@link CodecRegistry} instance to use.
   * It also uses the default maximum batch size (100).
   *
   * @param cluster The {@link Cluster} to use; cannot be {@code null}.
   */
  public StatementBatcher(Cluster cluster) {
    this(cluster, BatchMode.PARTITION_KEY);
  }

  /**
   * Creates a new {@link StatementBatcher} that produces {@link
   * com.datastax.driver.core.BatchStatement.Type#UNLOGGED unlogged} batches, operates in the
   * specified {@link BatchMode batch mode} and uses the given {@link Cluster} as its source for the
   * {@link ProtocolVersion protocol version} and the {@link CodecRegistry} instance to use. It also
   * uses the default maximum batch size (100).
   *
   * @param cluster The {@link Cluster} to use; cannot be {@code null}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   */
  public StatementBatcher(Cluster cluster, BatchMode batchMode) {
    this(cluster, batchMode, BatchStatement.Type.UNLOGGED, DEFAULT_MAX_BATCH_STATEMENTS);
  }

  /**
   * Creates a new {@link StatementBatcher} that produces batches of the given {@code batchType},
   * operates in the specified {@code batchMode} and uses the given {@link Cluster} as its source
   * for the {@link ProtocolVersion protocol version} and the {@link CodecRegistry} instance to use.
   *
   * @param cluster The {@link Cluster} to use; cannot be {@code null}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   * @param batchType The batch type to use; cannot be {@code null}.
   * @param maxBatchSize The maximum batch size; must be &gt; 1.
   */
  public StatementBatcher(
      Cluster cluster, BatchMode batchMode, BatchStatement.Type batchType, int maxBatchSize) {
    this.cluster = Objects.requireNonNull(cluster);
    this.batchMode = Objects.requireNonNull(batchMode);
    this.batchType = Objects.requireNonNull(batchType);
    protocolVersion = cluster.getConfiguration().getProtocolOptions().getProtocolVersion();
    codecRegistry = cluster.getConfiguration().getCodecRegistry();
    if (maxBatchSize <= 1) {
      throw new IllegalArgumentException("Maximum batch size must be greater than 1");
    }
    this.maxBatchSize = maxBatchSize;
    maxBatchStatements = DEFAULT_MAX_BATCH_STATEMENTS;
    maxSizeInBytes = DEFAULT_MAX_SIZE_BYTES;
  }
  //todo new constructor that supports both new params, and validate that they are properly set

  /**
   * Batches together the given statements into groups of statements having the same grouping key.
   *
   * <p>The grouping key to use is determined by the {@link BatchMode batch mode} in use by this
   * statement batcher.
   *
   * <p>When the number of statements for the same grouping key is greater than the maximum batch
   * size, statements will be split in different batches.
   *
   * <p>When {@link BatchMode#PARTITION_KEY PARTITION_KEY} is used, the grouping key is the
   * statement's {@link Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key} or
   * {@link Statement#getRoutingToken() routing token}, whichever is available.
   *
   * <p>When {@link BatchMode#REPLICA_SET REPLICA_SET} is used, the grouping key is the replica set
   * owning the statement's {@link Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing
   * key} or {@link Statement#getRoutingToken() routing token}, whichever is available.
   *
   * @param statements the statements to batch together.
   * @return A list of batched statements.
   */
  public List<Statement> batchByGroupingKey(Statement... statements) {
    return batchByGroupingKey(Arrays.asList(statements));
  }

  /**
   * Batches together the given statements into groups of statements having the same grouping key.
   *
   * <p>The grouping key to use is determined by the {@link BatchMode batch mode} in use by this
   * statement batcher.
   *
   * <p>When the number of statements for the same grouping key is greater than the maximum batch
   * size, statements will be split in different batches.
   *
   * <p>When {@link BatchMode#PARTITION_KEY PARTITION_KEY} is used, the grouping key is the
   * statement's {@link Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key} or
   * {@link Statement#getRoutingToken() routing token}, whichever is available.
   *
   * <p>When {@link BatchMode#REPLICA_SET REPLICA_SET} is used, the grouping key is the replica set
   * owning the statement's {@link Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing
   * key} or {@link Statement#getRoutingToken() routing token}, whichever is available.
   *
   * @param statements the statements to batch together.
   * @return A list of batched statements.
   */
  public List<Statement> batchByGroupingKey(Iterable<? extends Statement> statements) {
    return StreamSupport.stream(statements.spliterator(), false)
        .collect(Collectors.groupingBy(this::groupingKey))
        .values()
        .stream()
        .flatMap(stmts -> maybeBatch(stmts).stream())
        .collect(Collectors.toList());
  }

  /**
   * Batches together all the given statements into one single {@link BatchStatement}.
   *
   * <p>Note that when given one single statement, this method will not create a batch statement
   * containing that single statement; instead, it will return that same statement.
   *
   * <p>When the number of given statements is greater than the maximum batch size, this method will
   * split them into different batches.
   *
   * <p>Use this method with caution; if the given statements do not share the same {@link
   * Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key}, the resulting batch could
   * lead to write throughput degradation.
   *
   * @param statements the statements to batch together.
   * @return A list of {@link BatchStatement}s containing all the given statements batched together,
   *     or the original statement, if only one was provided.
   */
  public List<Statement> batchAll(Statement... statements) {
    return batchAll(Arrays.asList(statements));
  }

  /**
   * Batches together all the given statements into one or more {@link BatchStatement}s.
   *
   * <p>Note that when given one single statement, this method will not create a batch statement
   * containing that single statement; instead, it will return that same statement.
   *
   * <p>When the number of given statements is greater than the maximum batch size, this method will
   * split them into different batches.
   *
   * <p>Use this method with caution; if the given statements do not share the same {@link
   * Statement#getRoutingKey(ProtocolVersion, CodecRegistry) routing key}, the resulting batch could
   * lead to write throughput degradation.
   *
   * @param statements the statements to batch together.
   * @return A list of {@link BatchStatement}s containing all the given statements batched together,
   *     or the original statement, if only one was provided.
   */
  public List<Statement> batchAll(Collection<? extends Statement> statements) {
    return maybeBatch(statements);
  }

  private List<Statement> maybeBatch(Collection<? extends Statement> stmts) {
    Objects.requireNonNull(stmts);
    Preconditions.checkArgument(!stmts.isEmpty());
    // Don't wrap single statements in batch.
    if (stmts.size() == 1) {
      return Collections.singletonList(stmts.iterator().next());
    } else {
      List<Statement> batches = new ArrayList<>(stmts.size() / maxBatchSize);
      for (List<? extends Statement> chunk : Iterables.partition(stmts, maxBatchSize)) {
        BatchStatement batch = new BatchStatement(batchType);
        chunk.forEach(batch::add);
        batches.add(batch);
      }
      return batches;
    }
  }

  protected Object groupingKey(Statement statement) {
    Token routingToken = statement.getRoutingToken();
    ByteBuffer routingKey = statement.getRoutingKey(protocolVersion, codecRegistry);
    switch (batchMode) {
      case REPLICA_SET:
        String keyspace = statement.getKeyspace();
        if (keyspace != null) {
          Set<Host> replicas = null;
          if (routingKey != null) {
            replicas = cluster.getMetadata().getReplicas(keyspace, routingKey);
          } else if (routingToken != null) {
            replicas = cluster.getMetadata().getReplicas(keyspace, routingToken);
          }
          if (replicas != null && !replicas.isEmpty()) {
            return replicas.hashCode();
          }
        }
        // fall-through

      case PARTITION_KEY:
        if (routingToken != null) {
          return routingToken;
        } else if (routingKey != null && routingKey.hasRemaining()) {
          return routingKey;
        }
    }
    // If no grouping key can be discerned, return the statement itself so it
    // will stay unbatched.
    return statement;
  }
}
