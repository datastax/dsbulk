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
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.dsbulk.connectors.api.Record;
import com.datastax.oss.dsbulk.sampler.SizeableBoundStatement;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Map;

public class MappedBoundStatement extends SizeableBoundStatement implements MappedStatement {

  private final Record source;

  public MappedBoundStatement(Record source, BoundStatement delegate) {
    super(delegate);
    this.source = source;
  }

  @Override
  public @NonNull Record getRecord() {
    return source;
  }

  @NonNull
  @Override
  public MappedBoundStatement setExecutionProfileName(String newConfigProfileName) {
    super.setExecutionProfileName(newConfigProfileName);
    return this;
  }

  @NonNull
  @Override
  public MappedBoundStatement setExecutionProfile(DriverExecutionProfile newProfile) {
    super.setExecutionProfile(newProfile);
    return this;
  }

  @NonNull
  @Override
  public MappedBoundStatement setRoutingKeyspace(CqlIdentifier newRoutingKeyspace) {
    super.setRoutingKeyspace(newRoutingKeyspace);
    return this;
  }

  @NonNull
  @Override
  public MappedBoundStatement setRoutingKey(ByteBuffer newRoutingKey) {
    super.setRoutingKey(newRoutingKey);
    return this;
  }

  @NonNull
  @Override
  public MappedBoundStatement setRoutingToken(Token newRoutingToken) {
    super.setRoutingToken(newRoutingToken);
    return this;
  }

  @NonNull
  @Override
  public MappedBoundStatement setCustomPayload(@NonNull Map<String, ByteBuffer> newCustomPayload) {
    super.setCustomPayload(newCustomPayload);
    return this;
  }

  @NonNull
  @Override
  public MappedBoundStatement setIdempotent(Boolean newIdempotence) {
    super.setIdempotent(newIdempotence);
    return this;
  }

  @NonNull
  @Override
  public MappedBoundStatement setTracing(boolean newTracing) {
    super.setTracing(newTracing);
    return this;
  }

  @NonNull
  @Override
  public MappedBoundStatement setQueryTimestamp(long newTimestamp) {
    super.setQueryTimestamp(newTimestamp);
    return this;
  }

  @NonNull
  @Override
  public MappedBoundStatement setTimeout(Duration newTimeout) {
    super.setTimeout(newTimeout);
    return this;
  }

  @NonNull
  @Override
  public MappedBoundStatement setPagingState(ByteBuffer newPagingState) {
    super.setPagingState(newPagingState);
    return this;
  }

  @NonNull
  @Override
  public MappedBoundStatement setPageSize(int newPageSize) {
    super.setPageSize(newPageSize);
    return this;
  }

  @NonNull
  @Override
  public MappedBoundStatement setConsistencyLevel(ConsistencyLevel newConsistencyLevel) {
    super.setConsistencyLevel(newConsistencyLevel);
    return this;
  }

  @NonNull
  @Override
  public MappedBoundStatement setSerialConsistencyLevel(
      ConsistencyLevel newSerialConsistencyLevel) {
    super.setSerialConsistencyLevel(newSerialConsistencyLevel);
    return this;
  }

  @NonNull
  @Override
  public MappedBoundStatement setBytesUnsafe(int i, ByteBuffer v) {
    super.setBytesUnsafe(i, v);
    return this;
  }

  @NonNull
  @Override
  public MappedBoundStatement setNode(Node node) {
    super.setNode(node);
    return this;
  }
}
