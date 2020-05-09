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
package com.datastax.oss.dsbulk.executor.api.batch;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import edu.umd.cs.findbugs.annotations.NonNull;

public interface ReactiveStatementBatcherFactory {

  /** The default maximum number of statements that a batch can contain. */
  int DEFAULT_MAX_BATCH_STATEMENTS = 100;

  /** The default maximum data size in bytes that a batch can contain (unlimited). */
  long DEFAULT_MAX_SIZE_BYTES = -1;

  /**
   * Creates a new {@link ReactiveStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in {@linkplain BatchMode#PARTITION_KEY
   * partition key} mode and uses the {@linkplain ProtocolVersion#DEFAULT latest stable} protocol
   * version and the default {@link CodecRegistry#DEFAULT CodecRegistry} instance. It also uses the
   * default {@linkplain #DEFAULT_MAX_BATCH_STATEMENTS maximum number of statements} (100) and the
   * default {@linkplain #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited).
   */
  ReactiveStatementBatcher create();

  /**
   * Creates a new {@link ReactiveStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in {@linkplain BatchMode#PARTITION_KEY
   * partition key} mode and uses the {@linkplain ProtocolVersion#DEFAULT latest stable} protocol
   * version and the default {@link CodecRegistry#DEFAULT CodecRegistry} instance and the default
   * {@linkplain #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited). It uses the given
   * maximum number of statements.
   *
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   */
  ReactiveStatementBatcher create(int maxBatchStatements);

  /**
   * Creates a new {@link ReactiveStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in {@linkplain BatchMode#PARTITION_KEY
   * partition key} mode and uses the {@linkplain ProtocolVersion#DEFAULT latest stable} protocol
   * version and the default {@link CodecRegistry#DEFAULT CodecRegistry} instance and the default
   * {@linkplain #DEFAULT_MAX_BATCH_STATEMENTS maximum number of statements} (100). It uses the
   * given maximum data size in bytes.
   *
   * @param maxSizeInBytes The maximum number of bytes of data in one batch. If set to zero or any
   *     negative value, the data size is considered unlimited.
   */
  ReactiveStatementBatcher create(long maxSizeInBytes);

  /**
   * Creates a new {@link ReactiveStatementBatcher} that produces {@linkplain
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
  ReactiveStatementBatcher create(int maxBatchStatements, long maxSizeInBytes);

  /**
   * Creates a new {@link ReactiveStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in {@linkplain BatchMode#PARTITION_KEY
   * partition key} mode and uses the given {@linkplain CqlSession session} as its source for the
   * {@linkplain ProtocolVersion protocol version} and the {@link CodecRegistry} instance to use. It
   * also uses the default {@linkplain #DEFAULT_MAX_BATCH_STATEMENTS maximum number of statements}
   * (100) and the default {@linkplain #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes}
   * (unlimited).
   *
   * @param session The {@linkplain CqlSession session} to use; cannot be {@code null}.
   */
  default ReactiveStatementBatcher create(@NonNull CqlSession session) {
    return create(session, BatchMode.PARTITION_KEY);
  }

  /**
   * Creates a new {@link ReactiveStatementBatcher} that produces {@linkplain
   * DefaultBatchType#UNLOGGED unlogged} batches, operates in the specified {@linkplain BatchMode
   * batch mode} and uses the given {@linkplain CqlSession session} as its source for the
   * {@linkplain ProtocolVersion protocol version} and the {@link CodecRegistry} instance to use. It
   * also uses the default {@linkplain #DEFAULT_MAX_BATCH_STATEMENTS maximum number of statements}
   * (100) and the default {@linkplain #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes}
   * (unlimited).
   *
   * @param session The {@linkplain CqlSession session} to use; cannot be {@code null}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   */
  default ReactiveStatementBatcher create(
      @NonNull CqlSession session, @NonNull BatchMode batchMode) {
    return create(session, batchMode, DefaultBatchType.UNLOGGED, DEFAULT_MAX_BATCH_STATEMENTS);
  }

  /**
   * Creates a new {@link ReactiveStatementBatcher} that produces batches of the given {@code
   * batchType}, operates in the specified {@code batchMode} and uses the given {@linkplain
   * CqlSession session} as its source for the {@linkplain ProtocolVersion protocol version} and the
   * {@link CodecRegistry} instance to use. It uses the given maximum number of statements and the
   * default {@linkplain #DEFAULT_MAX_SIZE_BYTES maximum data size in bytes} (unlimited).
   *
   * @param session The {@linkplain CqlSession session} to use; cannot be {@code null}.
   * @param batchMode The batch mode to use; cannot be {@code null}.
   * @param batchType The batch type to use; cannot be {@code null}.
   * @param maxBatchStatements The maximum number of statements in a batch. If set to zero or any
   *     negative value, the number of statements is considered unlimited.
   */
  default ReactiveStatementBatcher create(
      @NonNull CqlSession session,
      @NonNull BatchMode batchMode,
      @NonNull BatchType batchType,
      int maxBatchStatements) {
    return create(session, batchMode, batchType, maxBatchStatements, DEFAULT_MAX_SIZE_BYTES);
  }

  /**
   * Creates a new {@link ReactiveStatementBatcher} that produces batches of the given {@code
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
  ReactiveStatementBatcher create(
      @NonNull CqlSession session,
      @NonNull BatchMode batchMode,
      @NonNull BatchType batchType,
      int maxBatchStatements,
      long maxSizeInBytes);
}
