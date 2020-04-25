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
package com.datastax.oss.dsbulk.workflow.commons.statement;

import com.datastax.oss.driver.api.core.ConsistencyLevel;
import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
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

public class BulkBoundStatement<T> implements BoundStatement, BulkStatement<T> {

  private final T source;
  private BoundStatement delegate;

  public BulkBoundStatement(T source, BoundStatement delegate) {
    this.source = source;
    this.delegate = delegate;
  }

  @Override
  public T getSource() {
    return source;
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
  public BoundStatement setExecutionProfileName(String newConfigProfileName) {
    delegate = delegate.setExecutionProfileName(newConfigProfileName);
    return this;
  }

  @Override
  public DriverExecutionProfile getExecutionProfile() {
    return delegate.getExecutionProfile();
  }

  @NonNull
  @Override
  public BoundStatement setExecutionProfile(DriverExecutionProfile newProfile) {
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
  public BoundStatement setRoutingKeyspace(CqlIdentifier newRoutingKeyspace) {
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
  public BoundStatement setRoutingKey(ByteBuffer newRoutingKey) {
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
  public BoundStatement setRoutingToken(Token newRoutingToken) {
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
  public BoundStatement setCustomPayload(@NonNull Map<String, ByteBuffer> newCustomPayload) {
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
  public BoundStatement setIdempotent(Boolean newIdempotence) {
    delegate = delegate.setIdempotent(newIdempotence);
    return this;
  }

  @Override
  public boolean isTracing() {
    return delegate.isTracing();
  }

  @NonNull
  @Override
  public BoundStatement setTracing(boolean newTracing) {
    delegate = delegate.setTracing(newTracing);
    return this;
  }

  @Override
  public long getQueryTimestamp() {
    return delegate.getQueryTimestamp();
  }

  @NonNull
  @Override
  public BoundStatement setQueryTimestamp(long newTimestamp) {
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
  public BoundStatement setTimeout(Duration newTimeout) {
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
  public BoundStatement setPagingState(ByteBuffer newPagingState) {
    delegate = delegate.setPagingState(newPagingState);
    return this;
  }

  @Override
  public int getPageSize() {
    return delegate.getPageSize();
  }

  @NonNull
  @Override
  public BoundStatement setPageSize(int newPageSize) {
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
  public BoundStatement setConsistencyLevel(ConsistencyLevel newConsistencyLevel) {
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
  public BoundStatement setSerialConsistencyLevel(ConsistencyLevel newSerialConsistencyLevel) {
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
  public BoundStatement setBytesUnsafe(int i, ByteBuffer v) {
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
  public BoundStatement setNode(Node node) {
    delegate = delegate.setNode(node);
    return this;
  }

  @Override
  public int computeSizeInBytes(@NonNull DriverContext context) {
    return delegate.computeSizeInBytes(context);
  }
}
