/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.connectors.api.internal;

import com.datastax.dsbulk.connectors.api.IndexedField;
import com.google.common.base.Preconditions;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;

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
  @NotNull
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
