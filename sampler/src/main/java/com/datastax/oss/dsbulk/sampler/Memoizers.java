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

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.Row;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.function.LongSupplier;

// Contents adapted from Guava's Suppliers.memoize()

class StatementSizeMemoizer implements LongSupplier {

  private volatile Statement<?> stmt;
  private volatile ProtocolVersion version;
  private volatile CodecRegistry registry;
  private volatile boolean initialized;
  // "value" does not need to be volatile; visibility piggy-backs
  // on volatile read of "initialized".
  private long value;

  StatementSizeMemoizer(
      @NonNull Statement<?> stmt,
      @NonNull ProtocolVersion version,
      @NonNull CodecRegistry registry) {
    this.stmt = stmt;
    this.version = version;
    this.registry = registry;
  }

  @Override
  public long getAsLong() {
    // A 2-field variant of Double Checked Locking.
    if (!initialized) {
      synchronized (this) {
        if (!initialized) {
          long t = DataSizes.getDataSize(stmt, version, registry);
          value = t;
          initialized = true;
          // Release the delegates to GC.
          stmt = null;
          version = null;
          registry = null;
          return t;
        }
      }
    }
    return value;
  }
}

class RowSizeMemoizer implements LongSupplier {

  private volatile Row row;
  private volatile boolean initialized;
  // "value" does not need to be volatile; visibility piggy-backs
  // on volatile read of "initialized".
  private long value;

  RowSizeMemoizer(@NonNull Row row) {
    this.row = row;
  }

  @Override
  public long getAsLong() {
    // A 2-field variant of Double Checked Locking.
    if (!initialized) {
      synchronized (this) {
        if (!initialized) {
          long t = DataSizes.getDataSize(row);
          value = t;
          initialized = true;
          // Release the delegate to GC.
          row = null;
          return t;
        }
      }
    }
    return value;
  }
}
