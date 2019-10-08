/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.statement;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import java.nio.ByteBuffer;
import java.util.Map;
import org.jetbrains.annotations.NotNull;

public class WrappedStatement<T extends Statement<T>> implements Statement<T> {

  private final Statement<T> wrapped;

  WrappedStatement(Statement<T> wrapped) {
    this.wrapped = wrapped;
  }

  @NotNull
  @Override
  public T setConfigProfileName(String newConfigProfileName) {
    return wrapped.setConfigProfileName(newConfigProfileName);
  }

  @NotNull
  @Override
  public T setConfigProfile(DriverConfigProfile newProfile) {
    return wrapped.setConfigProfile(newProfile);
  }

  @NotNull
  @Override
  public T setRoutingKeyspace(CqlIdentifier newRoutingKeyspace) {
    return wrapped.setRoutingKeyspace(newRoutingKeyspace);
  }

  @NotNull
  @Override
  public T setRoutingKey(ByteBuffer newRoutingKey) {
    return wrapped.setRoutingKey(newRoutingKey);
  }

  @NotNull
  @Override
  public T setRoutingToken(Token newRoutingToken) {
    return wrapped.setRoutingToken(newRoutingToken);
  }

  @NotNull
  @Override
  public T setIdempotent(Boolean newIdempotence) {
    return wrapped.setIdempotent(newIdempotence);
  }

  @NotNull
  @Override
  public T setTracing(boolean newTracing) {
    return wrapped.setTracing(newTracing);
  }

  @Override
  public long getTimestamp() {
    return wrapped.getTimestamp();
  }

  @NotNull
  @Override
  public T setTimestamp(long newTimestamp) {
    return wrapped.setTimestamp(newTimestamp);
  }

  @Override
  public ByteBuffer getPagingState() {
    return wrapped.getPagingState();
  }

  @NotNull
  @Override
  public T setPagingState(ByteBuffer newPagingState) {
    return wrapped.setPagingState(newPagingState);
  }

  @NotNull
  @Override
  public T setCustomPayload(@NotNull Map<String, ByteBuffer> newCustomPayload) {
    return wrapped.setCustomPayload(newCustomPayload);
  }

  @Override
  public String getConfigProfileName() {
    return wrapped.getConfigProfileName();
  }

  @Override
  public DriverConfigProfile getConfigProfile() {
    return wrapped.getConfigProfile();
  }

  @Override
  public CqlIdentifier getRoutingKeyspace() {
    return wrapped.getRoutingKeyspace();
  }

  @Override
  public ByteBuffer getRoutingKey() {
    return wrapped.getRoutingKey();
  }

  @Override
  public Token getRoutingToken() {
    return wrapped.getRoutingToken();
  }

  @NotNull
  @Override
  public Map<String, ByteBuffer> getCustomPayload() {
    return wrapped.getCustomPayload();
  }

  @Override
  public Boolean isIdempotent() {
    return wrapped.isIdempotent();
  }

  @Override
  public boolean isTracing() {
    return wrapped.isTracing();
  }

  @Override
  public int computeSizeInBytes(@NotNull DriverContext context) {
    return wrapped.computeSizeInBytes(context);
  }

  @Override
  public CqlIdentifier getKeyspace() {
    return wrapped.getKeyspace();
  }
}
