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
package com.datastax.oss.dsbulk.connectors.api;

import com.datastax.oss.driver.shaded.guava.common.base.Preconditions;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;

public class DefaultIndexedField implements IndexedField {

  private final int index;

  public DefaultIndexedField(int index) {
    Preconditions.checkArgument(index >= 0);
    this.index = index;
  }

  @Override
  public int getFieldIndex() {
    return index;
  }

  @Override
  @NonNull
  public String getFieldDescription() {
    return Integer.toString(index);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    // implementation note: it is important to consider other implementations of IndexedField
    if (!(o instanceof IndexedField)) {
      return false;
    }
    IndexedField that = (IndexedField) o;
    return index == that.getFieldIndex();
  }

  @Override
  public int hashCode() {
    return Objects.hash(index);
  }

  @Override
  public String toString() {
    return getFieldDescription();
  }
}
