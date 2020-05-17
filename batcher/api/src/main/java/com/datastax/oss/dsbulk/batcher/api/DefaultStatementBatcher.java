/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.batcher.api;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.dsbulk.sampler.DataSizes;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class DefaultStatementBatcher implements StatementBatcher {

  protected final CqlSession session;
  protected final BatchMode batchMode;
  protected final BatchType batchType;
  protected final ProtocolVersion protocolVersion;
  protected final CodecRegistry codecRegistry;
  protected final int maxBatchStatements;
  protected final long maxSizeInBytes;

  /**
   * Creates a new {@link DefaultStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in {@linkplain BatchMode#PARTITION_KEY
   * partition key} mode and uses the default {@linkplain
   * ReactiveStatementBatcherFactory#DEFAULT_MAX_BATCH_STATEMENTS maximum number of statements}
   * (100) and the default {@linkplain ReactiveStatementBatcherFactory#DEFAULT_MAX_SIZE_BYTES
   * maximum data size in bytes} (unlimited).
   */
  protected DefaultStatementBatcher() {
    this.session = null;
    this.batchMode = BatchMode.PARTITION_KEY;
    this.batchType = DefaultBatchType.UNLOGGED;
    this.protocolVersion = ProtocolVersion.DEFAULT;
    this.codecRegistry = CodecRegistry.DEFAULT;
    this.maxBatchStatements = ReactiveStatementBatcherFactory.DEFAULT_MAX_BATCH_STATEMENTS;
    this.maxSizeInBytes = ReactiveStatementBatcherFactory.DEFAULT_MAX_SIZE_BYTES;
  }

  /**
   * Creates a new {@link DefaultStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in {@linkplain BatchMode#PARTITION_KEY
   * partition key} mode and uses the default {@linkplain
   * ReactiveStatementBatcherFactory#DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited).
   * It uses the given maximum number of statements.
   *
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   */
  public DefaultStatementBatcher(int maxBatchStatements) {
    this.session = null;
    this.batchMode = BatchMode.PARTITION_KEY;
    this.batchType = DefaultBatchType.UNLOGGED;
    this.protocolVersion = ProtocolVersion.DEFAULT;
    this.codecRegistry = CodecRegistry.DEFAULT;
    this.maxBatchStatements = maxBatchStatements;
    this.maxSizeInBytes = ReactiveStatementBatcherFactory.DEFAULT_MAX_SIZE_BYTES;
  }

  /**
   * Creates a new {@link DefaultStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in {@linkplain BatchMode#PARTITION_KEY
   * partition key} mode and uses the default {@linkplain
   * ReactiveStatementBatcherFactory#DEFAULT_MAX_BATCH_STATEMENTS maximum number of statements}
   * (100). It uses the given maximum data size in bytes.
   *
   * @param maxSizeInBytes The maximum number of bytes of data in one batch. If set to zero or any
   *     negative value, the data size is considered unlimited.
   */
  public DefaultStatementBatcher(long maxSizeInBytes) {
    this.session = null;
    this.batchMode = BatchMode.PARTITION_KEY;
    this.batchType = DefaultBatchType.UNLOGGED;
    this.protocolVersion = ProtocolVersion.DEFAULT;
    this.codecRegistry = CodecRegistry.DEFAULT;
    this.maxBatchStatements = ReactiveStatementBatcherFactory.DEFAULT_MAX_BATCH_STATEMENTS;
    this.maxSizeInBytes = maxSizeInBytes;
  }

  /**
   * Creates a new {@link DefaultStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in {@linkplain BatchMode#PARTITION_KEY
   * partition key} mode and uses the given maximum number of statements and the maximum data size
   * in bytes.
   *
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   * @param maxSizeInBytes The maximum number of bytes of data in one batch. If set to zero or any
   *     negative value, the data size is considered unlimited.
   */
  public DefaultStatementBatcher(int maxBatchStatements, long maxSizeInBytes) {
    this.session = null;
    this.batchMode = BatchMode.PARTITION_KEY;
    this.batchType = DefaultBatchType.UNLOGGED;
    this.protocolVersion = ProtocolVersion.DEFAULT;
    this.codecRegistry = CodecRegistry.DEFAULT;
    this.maxBatchStatements = maxBatchStatements;
    this.maxSizeInBytes = maxSizeInBytes;
  }

  /**
   * Creates a new {@link DefaultStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in {@linkplain BatchMode#PARTITION_KEY
   * partition key} mode and uses the default {@linkplain
   * ReactiveStatementBatcherFactory#DEFAULT_MAX_BATCH_STATEMENTS maximum number of statements}
   * (100) and the default {@linkplain ReactiveStatementBatcherFactory#DEFAULT_MAX_SIZE_BYTES
   * maximum data size in bytes} (unlimited).
   *
   * @param session The {@linkplain CqlSession session} to use; cannot be {@code null} if {@code
   *     batchMode} is {@link BatchMode#REPLICA_SET REPLICA_SET}.
   */
  public DefaultStatementBatcher(@Nullable CqlSession session) {
    this(session, BatchMode.PARTITION_KEY);
  }

  /**
   * Creates a new {@link DefaultStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in the specified {@linkplain BatchMode
   * batch mode} and uses the given {@linkplain CqlSession session} as its source for the
   * {@linkplain ProtocolVersion protocol version} and the {@link CodecRegistry} instance to use. It
   * also uses the default {@linkplain ReactiveStatementBatcherFactory#DEFAULT_MAX_BATCH_STATEMENTS
   * maximum number of statements} (100) and the default {@linkplain
   * ReactiveStatementBatcherFactory#DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited).
   *
   * @param session The {@linkplain CqlSession session} to use; cannot be {@code null} if {@code
   *     batchMode} is {@link BatchMode#REPLICA_SET REPLICA_SET}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   */
  public DefaultStatementBatcher(@Nullable CqlSession session, @NonNull BatchMode batchMode) {
    this(
        session,
        batchMode,
        DefaultBatchType.UNLOGGED,
        ReactiveStatementBatcherFactory.DEFAULT_MAX_BATCH_STATEMENTS);
  }

  /**
   * Creates a new {@link DefaultStatementBatcher} that produces batches of the given {@code
   * batchType}, operates in the specified {@code batchMode} and uses the given {@linkplain
   * CqlSession session} as its source for the {@linkplain ProtocolVersion protocol version} and the
   * {@link CodecRegistry} instance to use. It also uses uses the given maximum number of statements
   * and the default {@linkplain ReactiveStatementBatcherFactory#DEFAULT_MAX_SIZE_BYTES maximum data
   * size in bytes} (unlimited).
   *
   * @param session The {@linkplain CqlSession session} to use; cannot be {@code null} if {@code
   *     batchMode} is {@link BatchMode#REPLICA_SET REPLICA_SET}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   * @param batchType The batch type to use; cannot be {@code null}.
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   */
  public DefaultStatementBatcher(
      @Nullable CqlSession session,
      @NonNull BatchMode batchMode,
      @NonNull BatchType batchType,
      int maxBatchStatements) {
    this(
        session,
        batchMode,
        batchType,
        maxBatchStatements,
        ReactiveStatementBatcherFactory.DEFAULT_MAX_SIZE_BYTES);
  }

  /**
   * Creates a new {@link DefaultStatementBatcher} that produces batches of the given {@code
   * batchType}, operates in the specified {@code batchMode} and uses the given {@linkplain
   * CqlSession session} as its source for the {@linkplain ProtocolVersion protocol version} and the
   * {@link CodecRegistry} instance to use. It uses the given maximum number of statements and the
   * maximum data size in bytes.
   *
   * @param session The {@linkplain CqlSession session} to use; cannot be {@code null} if {@code
   *     batchMode} is {@link BatchMode#REPLICA_SET REPLICA_SET}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   * @param batchType The batch type to use; cannot be {@code null}.
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   * @param maxSizeInBytes The maximum number of bytes of data in one batch. If set to zero or any
   *     negative value, the data size is considered unlimited.
   */
  public DefaultStatementBatcher(
      @Nullable CqlSession session,
      @NonNull BatchMode batchMode,
      @NonNull BatchType batchType,
      int maxBatchStatements,
      long maxSizeInBytes) {
    this.session = Objects.requireNonNull(session);
    this.batchMode = Objects.requireNonNull(batchMode);
    this.batchType = Objects.requireNonNull(batchType);
    this.protocolVersion = session.getContext().getProtocolVersion();
    this.codecRegistry = session.getContext().getCodecRegistry();
    if (maxBatchStatements < 0 && maxSizeInBytes < 0) {
      throw new IllegalArgumentException(
          "At least one of maxBatchStatements or maxSizeInBytes must be positive");
    }
    this.maxBatchStatements = maxBatchStatements;
    this.maxSizeInBytes = maxSizeInBytes;
  }

  @Override
  @NonNull
  public List<Statement<?>> batchByGroupingKey(
      @NonNull Iterable<BatchableStatement<?>> statements) {
    return StreamSupport.stream(statements.spliterator(), false)
        .collect(Collectors.groupingBy(this::groupingKey))
        .values()
        .stream()
        .flatMap(stmts -> maybeBatch(stmts).stream())
        .collect(Collectors.toList());
  }

  @Override
  @NonNull
  public List<Statement<?>> batchAll(@NonNull Collection<BatchableStatement<?>> statements) {
    return maybeBatch(statements);
  }

  @NonNull
  private List<Statement<?>> maybeBatch(@NonNull Collection<BatchableStatement<?>> stmts) {
    Objects.requireNonNull(stmts);
    Preconditions.checkArgument(!stmts.isEmpty());
    // Don't wrap single statements in batch.
    if (stmts.size() == 1) {
      if (stmts instanceof List) {
        List<BatchableStatement<?>> list = (List<BatchableStatement<?>>) stmts;
        return Collections.singletonList(list.get(0));
      } else {
        return Collections.singletonList(stmts.iterator().next());
      }
    } else {
      ImmutableList.Builder<Statement<?>> batches = ImmutableList.builder();
      List<BatchableStatement<?>> current = new ArrayList<>();
      AdaptiveSizingBatchPredicate shouldFlush = new AdaptiveSizingBatchPredicate();
      for (Iterator<? extends BatchableStatement<?>> it = stmts.iterator(); it.hasNext(); ) {
        BatchableStatement<?> stmt = it.next();
        current.add(stmt);
        if (shouldFlush.test(stmt)) {
          flush(current, batches);
          if (it.hasNext()) {
            current.clear();
          }
        }
      }
      if (current.size() > 0) {
        flush(current, batches);
      }
      return batches.build();
    }
  }

  private void flush(
      List<BatchableStatement<?>> current, ImmutableList.Builder<Statement<?>> batches) {
    if (current.size() == 1) {
      batches.add(current.get(0));
    } else {
      batches.add(BatchStatement.newInstance(batchType, current));
    }
  }

  @NonNull
  protected Object groupingKey(@NonNull Statement<?> statement) {
    Token routingToken = statement.getRoutingToken();
    ByteBuffer routingKey = statement.getRoutingKey();
    switch (batchMode) {
      case REPLICA_SET:
        CqlIdentifier keyspace = getKeyspace(statement);
        if (keyspace != null) {
          TokenMap tokenMap = session.getMetadata().getTokenMap().orElse(null);
          if (tokenMap != null) {
            Set<Node> replicas = null;
            if (routingKey != null) {
              replicas = tokenMap.getReplicas(keyspace, routingKey);
            } else if (routingToken != null) {
              replicas = tokenMap.getReplicas(keyspace, routingToken);
            }
            if (replicas != null && !replicas.isEmpty()) {
              return replicas.hashCode();
            }
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

  @Nullable
  private CqlIdentifier getKeyspace(Statement<?> statement) {
    if (statement.getKeyspace() != null) {
      return statement.getKeyspace();
    }
    if (statement.getRoutingKeyspace() != null) {
      return statement.getRoutingKeyspace();
    }
    return session.getKeyspace().orElse(null);
  }

  protected class AdaptiveSizingBatchPredicate implements Predicate<BatchableStatement<?>> {

    private int statementsCounter = 0;
    private long bytesInCurrentBatch = 0;

    @Override
    public boolean test(@NonNull BatchableStatement<?> statement) {
      boolean statementsOverflowBuffer = ++statementsCounter >= getMaxBatchStatements();
      boolean bytesOverflowBuffer =
          (bytesInCurrentBatch += calculateSize(statement)) >= getMaxSizeInBytes();
      boolean shouldFlush = statementsOverflowBuffer || bytesOverflowBuffer;
      if (shouldFlush) {
        statementsCounter = 0;
        bytesInCurrentBatch = 0;
      }
      return shouldFlush;
    }

    long calculateSize(@NonNull Statement<?> statement) {
      return DataSizes.getDataSize(statement, protocolVersion, codecRegistry);
    }

    int getMaxBatchStatements() {
      if (maxBatchStatements <= 0) {
        return Integer.MAX_VALUE;
      }
      return maxBatchStatements;
    }

    long getMaxSizeInBytes() {
      if (maxSizeInBytes <= 0) {
        return Long.MAX_VALUE;
      }
      return maxSizeInBytes;
    }
  }
}
