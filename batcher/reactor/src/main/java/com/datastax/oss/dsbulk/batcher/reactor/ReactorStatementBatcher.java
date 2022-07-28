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
package com.datastax.oss.dsbulk.batcher.reactor;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.dsbulk.batcher.api.BatchMode;
import com.datastax.oss.dsbulk.batcher.api.DefaultStatementBatcher;
import com.datastax.oss.dsbulk.batcher.api.ReactiveStatementBatcher;
import com.datastax.oss.dsbulk.batcher.api.ReactiveStatementBatcherFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.ArrayList;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

public class ReactorStatementBatcher extends DefaultStatementBatcher
    implements ReactiveStatementBatcher {

  /**
   * Creates a new {@link ReactorStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in {@linkplain BatchMode#PARTITION_KEY
   * partition key} mode and uses the {@linkplain ProtocolVersion#DEFAULT latest stable} protocol
   * version and the default {@link CodecRegistry#DEFAULT CodecRegistry} instance. It also uses the
   * default {@linkplain ReactiveStatementBatcherFactory#DEFAULT_MAX_BATCH_STATEMENTS maximum number
   * of statements} (100) and the default {@linkplain
   * ReactiveStatementBatcherFactory#DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited).
   */
  public ReactorStatementBatcher() {}

  /**
   * Creates a new {@link ReactorStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in {@linkplain BatchMode#PARTITION_KEY
   * partition key} mode and uses the {@linkplain ProtocolVersion#DEFAULT latest stable} protocol
   * version and the default {@link CodecRegistry#DEFAULT CodecRegistry} instance and the default
   * {@linkplain ReactiveStatementBatcherFactory#DEFAULT_MAX_SIZE_BYTES maximum data size in bytes}
   * (unlimited). It uses the given maximum number of statements.
   *
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   */
  public ReactorStatementBatcher(int maxBatchStatements) {
    super(maxBatchStatements);
  }

  /**
   * Creates a new {@link ReactorStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in {@linkplain BatchMode#PARTITION_KEY
   * partition key} mode and uses the {@linkplain ProtocolVersion#DEFAULT latest stable} protocol
   * version and the default {@link CodecRegistry#DEFAULT CodecRegistry} instance and the default
   * {@linkplain ReactiveStatementBatcherFactory#DEFAULT_MAX_BATCH_STATEMENTS maximum number of
   * statements} (100). It uses the given maximum data size in bytes.
   *
   * @param maxSizeInBytes The maximum number of bytes of data in one batch. If set to zero or any
   *     negative value, the data size is considered unlimited.
   */
  public ReactorStatementBatcher(long maxSizeInBytes) {
    super(maxSizeInBytes);
  }

  /**
   * Creates a new {@link ReactorStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in {@linkplain BatchMode#PARTITION_KEY
   * partition key} mode and uses the {@linkplain ProtocolVersion#DEFAULT latest stable} protocol
   * version and the default {@link CodecRegistry#DEFAULT CodecRegistry} instance. It uses the given
   * maximum number of statements and the maximum data size in bytes.
   *
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   * @param maxSizeInBytes The maximum number of bytes of data in one batch. If set to zero or any
   *     negative value, the data size is considered unlimited.
   */
  public ReactorStatementBatcher(int maxBatchStatements, long maxSizeInBytes) {
    super(maxBatchStatements, maxSizeInBytes);
  }

  /**
   * Creates a new {@link ReactorStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in {@linkplain BatchMode#PARTITION_KEY
   * partition key} mode and uses the given {@linkplain CqlSession session} as its source for the
   * {@linkplain ProtocolVersion protocol version} and the {@link CodecRegistry} instance to use. It
   * also uses the default {@linkplain ReactiveStatementBatcherFactory#DEFAULT_MAX_BATCH_STATEMENTS
   * maximum number of statements} (100) and the default {@linkplain
   * ReactiveStatementBatcherFactory#DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited).
   *
   * @param session The {@linkplain CqlSession session} to use; cannot be {@code null}.
   */
  public ReactorStatementBatcher(@NonNull CqlSession session) {
    super(session);
  }

  /**
   * Creates a new {@link ReactorStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in the specified {@linkplain BatchMode
   * batch mode} and uses the given {@linkplain CqlSession session} as its source for the
   * {@linkplain ProtocolVersion protocol version} and the {@link CodecRegistry} instance to use. It
   * also uses the default {@linkplain ReactiveStatementBatcherFactory#DEFAULT_MAX_BATCH_STATEMENTS
   * maximum number of statements} (100) and the default {@linkplain
   * ReactiveStatementBatcherFactory#DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited).
   *
   * @param session The {@linkplain CqlSession session} to use; cannot be {@code null}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   */
  public ReactorStatementBatcher(@NonNull CqlSession session, @NonNull BatchMode batchMode) {
    super(session, batchMode);
  }

  /**
   * Creates a new {@link ReactorStatementBatcher} that produces batches of the given {@code
   * batchType}, operates in the specified {@code batchMode} and uses the given {@linkplain
   * CqlSession session} as its source for the {@linkplain ProtocolVersion protocol version} and the
   * {@link CodecRegistry} instance to use. It uses the given maximum number of statements and the
   * default {@linkplain ReactiveStatementBatcherFactory#DEFAULT_MAX_SIZE_BYTES maximum data size in
   * bytes} (unlimited).
   *
   * @param session The {@linkplain CqlSession session} to use; cannot be {@code null}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   * @param batchType The batch type to use; cannot be {@code null}.
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   */
  public ReactorStatementBatcher(
      @NonNull CqlSession session,
      @NonNull BatchMode batchMode,
      @NonNull BatchType batchType,
      int maxBatchStatements) {
    super(session, batchMode, batchType, maxBatchStatements);
  }

  /**
   * Creates a new {@link ReactorStatementBatcher} that produces batches of the given {@code
   * batchType}, operates in the specified {@code batchMode} and uses the given {@linkplain
   * CqlSession session} as its source for the {@linkplain ProtocolVersion protocol version} and the
   * {@link CodecRegistry} instance to use. It uses the given maximum number of statements and the
   * maximum data size in bytes.
   *
   * @param session The {@linkplain CqlSession session} to use; cannot be {@code null}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   * @param batchType The batch type to use; cannot be {@code null}.
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   * @param maxSizeInBytes The maximum number of bytes of data in one batch. If set to zero or any
   *     negative value, the data size is considered unlimited.
   */
  public ReactorStatementBatcher(
      @NonNull CqlSession session,
      @NonNull BatchMode batchMode,
      @NonNull BatchType batchType,
      int maxBatchStatements,
      long maxSizeInBytes) {
    super(session, batchMode, batchType, maxBatchStatements, maxSizeInBytes);
  }

  @Override
  @NonNull
  public Flux<Statement<?>> batchByGroupingKey(
      @NonNull Publisher<BatchableStatement<?>> statements) {
    return Flux.from(statements).groupBy(this::groupingKey).flatMap(this::batchAll);
  }

  @Override
  @NonNull
  public Flux<Statement<?>> batchAll(@NonNull Publisher<BatchableStatement<?>> statements) {
    return Flux.from(statements)
        .windowUntil(new ReactorAdaptiveSizingBatchPredicate(), false)
        .flatMap(
            stmts ->
                stmts
                    .reduce(
                        new ArrayList<BatchableStatement<?>>(),
                        (children, child) -> {
                          children.add(child);
                          return children;
                        })
                    .map(
                        children ->
                            children.size() == 1
                                ? children.get(0)
                                : createBatchStatement(children)));
  }

  private class ReactorAdaptiveSizingBatchPredicate extends AdaptiveSizingBatchPredicate {}
}
