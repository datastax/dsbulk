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
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.LongSupplier;

public class SizeableBoundStatement implements BoundStatement, Sizeable {

  private BoundStatement delegate;
  private final LongSupplier dataSize;

  public SizeableBoundStatement(@NonNull BoundStatement delegate) {
    this.delegate = delegate;
    this.dataSize =
        new StatementSizeMemoizer(delegate, delegate.protocolVersion(), delegate.codecRegistry());
  }

  @Override
  public long getDataSize() {
    return dataSize.getAsLong();
  }

  @NonNull
  @Override
  public PreparedStatement getPreparedStatement() {
    return delegate.getPreparedStatement();
  }

  @NonNull
  @Override
  public List<ByteBuffer> getValues() {
    return delegate.getValues();
  }

  @Override
  public String getExecutionProfileName() {
    return delegate.getExecutionProfileName();
  }

  @NonNull
  @Override
  public SizeableBoundStatement setExecutionProfileName(String newConfigProfileName) {
    delegate = delegate.setExecutionProfileName(newConfigProfileName);
    return this;
  }

  @Override
  public DriverExecutionProfile getExecutionProfile() {
    return delegate.getExecutionProfile();
  }

  @NonNull
  @Override
  public SizeableBoundStatement setExecutionProfile(DriverExecutionProfile newProfile) {
    delegate = delegate.setExecutionProfile(newProfile);
    return this;
  }

  @Nullable
  @Override
  public CqlIdentifier getKeyspace() {
    return delegate.getKeyspace();
  }

  @Nullable
  @Override
  public CqlIdentifier getRoutingKeyspace() {
    return delegate.getRoutingKeyspace();
  }

  @NonNull
  @Override
  public SizeableBoundStatement setRoutingKeyspace(CqlIdentifier newRoutingKeyspace) {
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
  public SizeableBoundStatement setRoutingKey(ByteBuffer newRoutingKey) {
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
  public SizeableBoundStatement setRoutingToken(Token newRoutingToken) {
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
  public SizeableBoundStatement setCustomPayload(
      @NonNull Map<String, ByteBuffer> newCustomPayload) {
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
  public SizeableBoundStatement setIdempotent(Boolean newIdempotence) {
    delegate = delegate.setIdempotent(newIdempotence);
    return this;
  }

  @Override
  public boolean isTracing() {
    return delegate.isTracing();
  }

  @NonNull
  @Override
  public SizeableBoundStatement setTracing(boolean newTracing) {
    delegate = delegate.setTracing(newTracing);
    return this;
  }

  @Override
  public long getQueryTimestamp() {
    return delegate.getQueryTimestamp();
  }

  @NonNull
  @Override
  public SizeableBoundStatement setQueryTimestamp(long newTimestamp) {
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
  public SizeableBoundStatement setTimeout(Duration newTimeout) {
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
  public SizeableBoundStatement setPagingState(ByteBuffer newPagingState) {
    delegate = delegate.setPagingState(newPagingState);
    return this;
  }

  @Override
  public int getPageSize() {
    return delegate.getPageSize();
  }

  @NonNull
  @Override
  public SizeableBoundStatement setPageSize(int newPageSize) {
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
  public SizeableBoundStatement setConsistencyLevel(ConsistencyLevel newConsistencyLevel) {
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
  public SizeableBoundStatement setSerialConsistencyLevel(
      ConsistencyLevel newSerialConsistencyLevel) {
    delegate = delegate.setSerialConsistencyLevel(newSerialConsistencyLevel);
    return this;
  }

  @Nullable
  @Override
  public ByteBuffer getBytesUnsafe(int i) {
    return delegate.getBytesUnsafe(i);
  }

  @NonNull
  @Override
  public SizeableBoundStatement setBytesUnsafe(int i, ByteBuffer v) {
    delegate = delegate.setBytesUnsafe(i, v);
    return this;
  }

  @Override
  public int size() {
    return delegate.size();
  }

  @NonNull
  @Override
  public DataType getType(int i) {
    return delegate.getType(i);
  }

  @NonNull
  @Override
  public CodecRegistry codecRegistry() {
    return delegate.codecRegistry();
  }

  @NonNull
  @Override
  public ProtocolVersion protocolVersion() {
    return delegate.protocolVersion();
  }

  @Override
  public int firstIndexOf(@NonNull String name) {
    return delegate.firstIndexOf(name);
  }

  @Override
  public int firstIndexOf(@NonNull CqlIdentifier id) {
    return delegate.firstIndexOf(id);
  }

  @Nullable
  @Override
  public Node getNode() {
    return delegate.getNode();
  }

  @NonNull
  @Override
  public SizeableBoundStatement setNode(Node node) {
    delegate = delegate.setNode(node);
    return this;
  }
}
