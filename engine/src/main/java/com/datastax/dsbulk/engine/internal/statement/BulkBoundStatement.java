/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.statement;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import java.nio.ByteBuffer;
import java.util.List;

public class BulkBoundStatement<T> extends WrappedStatement<BoundStatement>
    implements BoundStatement, BulkStatement<T> {
  private final T source;

  public BulkBoundStatement(T source, BoundStatement wrapped) {
    super(wrapped);
    this.source = source;
  }

  @Override
  public T getSource() {
    return source;
  }

  @Override
  public PreparedStatement getPreparedStatement() {
    return wrapped.getPreparedStatement();
  }

  @Override
  public List<ByteBuffer> getValues() {
    return wrapped.getValues();
  }

  @Override
  public int firstIndexOf(CqlIdentifier id) {
    return wrapped.firstIndexOf(id);
  }

  @Override
  public int firstIndexOf(String name) {
    return wrapped.firstIndexOf(name);
  }

  @Override
  public ByteBuffer getBytesUnsafe(int i) {
    return wrapped.getBytesUnsafe(i);
  }

  @Override
  public BoundStatement setBytesUnsafe(int i, ByteBuffer v) {
    return wrapped.setBytesUnsafe(i, v);
  }

  @Override
  public int size() {
    return wrapped.size();
  }

  @Override
  public DataType getType(int i) {
    return wrapped.getType(i);
  }

  @Override
  public CodecRegistry codecRegistry() {
    return wrapped.codecRegistry();
  }

  @Override
  public ProtocolVersion protocolVersion() {
    return wrapped.protocolVersion();
  }
}
