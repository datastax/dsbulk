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
import com.datastax.oss.driver.api.core.config.DriverExecutionProfile;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.dsbulk.connectors.api.Record;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Map;

public class MappedSimpleStatement implements SimpleStatement, MappedStatement {

  private final Record source;
  private SimpleStatement delegate;

  public MappedSimpleStatement(Record source, SimpleStatement delegate) {
    this.source = source;
    this.delegate = delegate;
  }

  @Override
  @NonNull
  public Record getRecord() {
    return source;
  }

  @NonNull
  @Override
  public String getQuery() {
    return delegate.getQuery();
  }

  @NonNull
  @Override
  public SimpleStatement setQuery(@NonNull String newQuery) {
    delegate = delegate.setQuery(newQuery);
    return this;
  }

  @NonNull
  @Override
  public SimpleStatement setKeyspace(CqlIdentifier newKeyspace) {
    delegate = delegate.setKeyspace(newKeyspace);
    return this;
  }

  @NonNull
  @Override
  public List<Object> getPositionalValues() {
    return delegate.getPositionalValues();
  }

  @NonNull
  @Override
  public SimpleStatement setPositionalValues(@NonNull List<Object> newPositionalValues) {
    delegate = delegate.setPositionalValues(newPositionalValues);
    return this;
  }

  @NonNull
  @Override
  public Map<CqlIdentifier, Object> getNamedValues() {
    return delegate.getNamedValues();
  }

  @NonNull
  @Override
  public SimpleStatement setNamedValuesWithIds(@NonNull Map<CqlIdentifier, Object> newNamedValues) {
    delegate = delegate.setNamedValuesWithIds(newNamedValues);
    return this;
  }

  @NonNull
  @Override
  public SimpleStatement setExecutionProfileName(String newConfigProfileName) {
    delegate = delegate.setExecutionProfileName(newConfigProfileName);
    return this;
  }

  @NonNull
  @Override
  public SimpleStatement setExecutionProfile(DriverExecutionProfile newProfile) {
    delegate = delegate.setExecutionProfile(newProfile);
    return this;
  }

  @NonNull
  @Override
  public SimpleStatement setRoutingKeyspace(CqlIdentifier newRoutingKeyspace) {
    delegate = delegate.setRoutingKeyspace(newRoutingKeyspace);
    return this;
  }

  @NonNull
  @Override
  public SimpleStatement setNode(Node node) {
    delegate = delegate.setNode(node);
    return this;
  }

  @NonNull
  @Override
  public SimpleStatement setRoutingKey(ByteBuffer newRoutingKey) {
    delegate = delegate.setRoutingKey(newRoutingKey);
    return this;
  }

  @NonNull
  @Override
  public SimpleStatement setRoutingToken(Token newRoutingToken) {
    delegate = delegate.setRoutingToken(newRoutingToken);
    return this;
  }

  @NonNull
  @Override
  public SimpleStatement setCustomPayload(@NonNull Map<String, ByteBuffer> newCustomPayload) {
    delegate = delegate.setCustomPayload(newCustomPayload);
    return this;
  }

  @NonNull
  @Override
  public SimpleStatement setIdempotent(Boolean newIdempotence) {
    delegate = delegate.setIdempotent(newIdempotence);
    return this;
  }

  @NonNull
  @Override
  public SimpleStatement setTracing(boolean newTracing) {
    delegate = delegate.setTracing(newTracing);
    return this;
  }

  @Override
  public long getQueryTimestamp() {
    return delegate.getQueryTimestamp();
  }

  @NonNull
  @Override
  public SimpleStatement setQueryTimestamp(long newTimestamp) {
    delegate = delegate.setQueryTimestamp(newTimestamp);
    return this;
  }

  @NonNull
  @Override
  public SimpleStatement setTimeout(Duration newTimeout) {
    delegate = delegate.setTimeout(newTimeout);
    return this;
  }

  @Override
  public ByteBuffer getPagingState() {
    return delegate.getPagingState();
  }

  @NonNull
  @Override
  public SimpleStatement setPagingState(ByteBuffer newPagingState) {
    delegate = delegate.setPagingState(newPagingState);
    return this;
  }

  @Override
  public int getPageSize() {
    return delegate.getPageSize();
  }

  @NonNull
  @Override
  public SimpleStatement setPageSize(int newPageSize) {
    delegate = delegate.setPageSize(newPageSize);
    return this;
  }

  @Override
  public ConsistencyLevel getConsistencyLevel() {
    return delegate.getConsistencyLevel();
  }

  @NonNull
  @Override
  public SimpleStatement setConsistencyLevel(ConsistencyLevel newConsistencyLevel) {
    delegate = delegate.setConsistencyLevel(newConsistencyLevel);
    return this;
  }

  @Override
  public ConsistencyLevel getSerialConsistencyLevel() {
    return delegate.getSerialConsistencyLevel();
  }

  @NonNull
  @Override
  public SimpleStatement setSerialConsistencyLevel(ConsistencyLevel newSerialConsistencyLevel) {
    delegate = delegate.setSerialConsistencyLevel(newSerialConsistencyLevel);
    return this;
  }

  @Override
  public boolean isTracing() {
    return delegate.isTracing();
  }

  @Override
  public String getExecutionProfileName() {
    return delegate.getExecutionProfileName();
  }

  @Override
  public DriverExecutionProfile getExecutionProfile() {
    return delegate.getExecutionProfile();
  }

  @Override
  public CqlIdentifier getKeyspace() {
    return delegate.getKeyspace();
  }

  @Override
  public CqlIdentifier getRoutingKeyspace() {
    return delegate.getRoutingKeyspace();
  }

  @Override
  public ByteBuffer getRoutingKey() {
    return delegate.getRoutingKey();
  }

  @Override
  public Token getRoutingToken() {
    return delegate.getRoutingToken();
  }

  @NonNull
  @Override
  public Map<String, ByteBuffer> getCustomPayload() {
    return delegate.getCustomPayload();
  }

  @Override
  public Boolean isIdempotent() {
    return delegate.isIdempotent();
  }

  @Override
  public Duration getTimeout() {
    return delegate.getTimeout();
  }

  @Override
  public Node getNode() {
    return delegate.getNode();
  }
}
