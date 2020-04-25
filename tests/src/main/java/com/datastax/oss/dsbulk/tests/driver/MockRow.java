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
package com.datastax.oss.dsbulk.tests.driver;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import java.nio.ByteBuffer;

@SuppressWarnings("NullableProblems")
public final class MockRow implements Row {

  private int index;

  public MockRow(int index) {
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
