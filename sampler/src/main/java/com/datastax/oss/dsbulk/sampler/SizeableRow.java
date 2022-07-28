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

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.detach.AttachmentPoint;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.nio.ByteBuffer;
import java.util.function.LongSupplier;

public class SizeableRow implements Row, Sizeable {

  private final Row delegate;
  private final LongSupplier dataSize;

  public SizeableRow(@NonNull Row delegate) {
    this.delegate = delegate;
    this.dataSize = new RowSizeMemoizer(delegate);
  }

  @Override
  public long getDataSize() {
    return dataSize.getAsLong();
  }

  @Override
  @NonNull
  public ColumnDefinitions getColumnDefinitions() {
    return delegate.getColumnDefinitions();
  }

  @Override
  @Nullable
  public ByteBuffer getBytesUnsafe(int i) {
    return delegate.getBytesUnsafe(i);
  }

  @Override
  public int firstIndexOf(@NonNull CqlIdentifier id) {
    return delegate.firstIndexOf(id);
  }

  @Override
  public int firstIndexOf(@NonNull String name) {
    return delegate.firstIndexOf(name);
  }

  @NonNull
  @Override
  public DataType getType(@NonNull CqlIdentifier id) {
    return delegate.getType(id);
  }

  @NonNull
  @Override
  public DataType getType(@NonNull String name) {
    return delegate.getType(name);
  }

  @NonNull
  @Override
  public DataType getType(int i) {
    return delegate.getType(i);
  }

  @Override
  public int size() {
    return delegate.size();
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
  public boolean isDetached() {
    return delegate.isDetached();
  }

  @Override
  public void attach(@NonNull AttachmentPoint attachmentPoint) {
    delegate.attach(attachmentPoint);
  }
}
