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
package com.datastax.oss.dsbulk.sampler;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Iterator;
import java.util.Map;
import java.util.function.LongSupplier;

public class SizeableBatchStatement implements BatchStatement, Sizeable {

  private BatchStatement delegate;
  private final LongSupplier dataSize;

  public SizeableBatchStatement(
      @NonNull BatchStatement delegate,
      @NonNull ProtocolVersion version,
      @NonNull CodecRegistry registry) {
    this.delegate = delegate;
    this.dataSize = new StatementSizeMemoizer(delegate, version, registry);
  }

  @Override
  public long getDataSize() {
    return dataSize.getAsLong();
  }

  @Override
  public String getExecutionProfileName() {
    return delegate.getExecutionProfileName();
  }

  @NonNull
  @Override
  public BatchStatement setExecutionProfileName(String newConfigProfileName) {
    delegate = delegate.setExecutionProfileName(newConfigProfileName);
    return this;
  }

  @Override
  public DriverExecutionProfile getExecutionProfile() {
    return delegate.getExecutionProfile();
  }

  @NonNull
  @Override
  public BatchStatement setExecutionProfile(DriverExecutionProfile newProfile) {
    delegate = delegate.setExecutionProfile(newProfile);
    return this;
  }

  @NonNull
  @Override
  public BatchType getBatchType() {
    return delegate.getBatchType();
  }

  @NonNull
  @Override
  public BatchStatement setBatchType(@NonNull BatchType newBatchType) {
    delegate = delegate.setBatchType(newBatchType);
    return this;
  }

  @Nullable
  @Override
  public CqlIdentifier getKeyspace() {
    return delegate.getKeyspace();
  }

  @NonNull
  @Override
  public BatchStatement setKeyspace(@Nullable CqlIdentifier newKeyspace) {
    delegate = delegate.setKeyspace(newKeyspace);
    return this;
  }

  @Nullable
  @Override
  public CqlIdentifier getRoutingKeyspace() {
    return delegate.getRoutingKeyspace();
  }

  @NonNull
  @Override
  public BatchStatement setRoutingKeyspace(CqlIdentifier newRoutingKeyspace) {
    delegate = delegate.setRoutingKeyspace(newRoutingKeyspace);
    return this;
  }

  @Nullable
  @Override
  public ByteBuffer getRoutingKey() {
    return delegate.getRoutingKey();
  }

  @NonNull
  @Override
  public BatchStatement setRoutingKey(ByteBuffer newRoutingKey) {
    delegate = delegate.setRoutingKey(newRoutingKey);
    return this;
  }

  @Nullable
  @Override
  public Token getRoutingToken() {
    return delegate.getRoutingToken();
  }

  @NonNull
  @Override
  public BatchStatement setRoutingToken(Token newRoutingToken) {
    delegate = delegate.setRoutingToken(newRoutingToken);
    return this;
  }

  @NonNull
  @Override
  public Map<String, ByteBuffer> getCustomPayload() {
    return delegate.getCustomPayload();
  }

  @NonNull
  @Override
  public BatchStatement setCustomPayload(@NonNull Map<String, ByteBuffer> newCustomPayload) {
    delegate = delegate.setCustomPayload(newCustomPayload);
    return this;
  }

  @Nullable
  @Override
  public Boolean isIdempotent() {
    return delegate.isIdempotent();
  }

  @NonNull
  @Override
  public BatchStatement setIdempotent(Boolean newIdempotence) {
    delegate = delegate.setIdempotent(newIdempotence);
    return this;
  }

  @Override
  public boolean isTracing() {
    return delegate.isTracing();
  }

  @NonNull
  @Override
  public BatchStatement setTracing(boolean newTracing) {
    delegate = delegate.setTracing(newTracing);
    return this;
  }

  @Override
  public long getQueryTimestamp() {
    return delegate.getQueryTimestamp();
  }

  @NonNull
  @Override
  public BatchStatement setQueryTimestamp(long newTimestamp) {
    delegate = delegate.setQueryTimestamp(newTimestamp);
    return this;
  }

  @Nullable
  @Override
  public Duration getTimeout() {
    return delegate.getTimeout();
  }

  @NonNull
  @Override
  public BatchStatement setTimeout(Duration newTimeout) {
    delegate = delegate.setTimeout(newTimeout);
    return this;
  }

  @Nullable
  @Override
  public ByteBuffer getPagingState() {
    return delegate.getPagingState();
  }

  @NonNull
  @Override
  public BatchStatement setPagingState(ByteBuffer newPagingState) {
    delegate = delegate.setPagingState(newPagingState);
    return this;
  }

  @Override
  public int getPageSize() {
    return delegate.getPageSize();
  }

  @NonNull
  @Override
  public BatchStatement setPageSize(int newPageSize) {
    delegate = delegate.setPageSize(newPageSize);
    return this;
  }

  @Nullable
  @Override
  public ConsistencyLevel getConsistencyLevel() {
    return delegate.getConsistencyLevel();
  }

  @NonNull
  @Override
  public BatchStatement setConsistencyLevel(ConsistencyLevel newConsistencyLevel) {
    delegate = delegate.setConsistencyLevel(newConsistencyLevel);
    return this;
  }

  @Nullable
  @Override
  public ConsistencyLevel getSerialConsistencyLevel() {
    return delegate.getSerialConsistencyLevel();
  }

  @NonNull
  @Override
  public BatchStatement setSerialConsistencyLevel(ConsistencyLevel newSerialConsistencyLevel) {
    delegate = delegate.setSerialConsistencyLevel(newSerialConsistencyLevel);
    return this;
  }

  @Override
  public Iterator<BatchableStatement<?>> iterator() {
    return delegate.iterator();
  }

  @NonNull
  @Override
  public BatchStatement add(@NonNull BatchableStatement<?> statement) {
    delegate = delegate.add(statement);
    return this;
  }

  @NonNull
  @Override
  public BatchStatement addAll(@NonNull Iterable<? extends BatchableStatement<?>> statements) {
    delegate = delegate.addAll(statements);
    return this;
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @NonNull
  @Override
  public BatchStatement clear() {
    delegate = delegate.clear();
    return this;
  }

  @Nullable
  @Override
  public Node getNode() {
    return delegate.getNode();
  }

  @NonNull
  @Override
  public BatchStatement setNode(Node node) {
    delegate = delegate.setNode(node);
    return this;
  }

  @Override
  public int computeSizeInBytes(@NonNull DriverContext context) {
    return delegate.computeSizeInBytes(context);
  }
}
