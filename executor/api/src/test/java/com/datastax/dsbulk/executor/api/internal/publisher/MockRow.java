/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.internal.publisher;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import java.nio.ByteBuffer;

@SuppressWarnings("NullableProblems")
final class MockRow implements Row {

  private int index;

  MockRow(int index) {
    this.index = index;
  }

  @Override
  public ColumnDefinitions getColumnDefinitions() {
    return null;
  }

  @Override
  public int firstIndexOf(CqlIdentifier id) {
    return 0;
  }

  @Override
  public DataType getType(CqlIdentifier id) {
    return null;
  }

  @Override
  public int firstIndexOf(String name) {
    return 0;
  }

  @Override
  public DataType getType(String name) {
    return null;
  }

  @Override
  public ByteBuffer getBytesUnsafe(int i) {
    return null;
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public DataType getType(int i) {
    return null;
  }

  @Override
  public CodecRegistry codecRegistry() {
    return null;
  }

  @Override
  public ProtocolVersion protocolVersion() {
    return null;
  }

  @Override
  public boolean isDetached() {
    return false;
  }

  @Override
  public void attach(AttachmentPoint attachmentPoint) {}

  // equals and hashCode required for TCK tests that check that two subscribers
  // receive the exact same set of items.

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    MockRow mockRow = (MockRow) o;
    return index == mockRow.index;
  }

  @Override
  public int hashCode() {
    return index;
  }
}
