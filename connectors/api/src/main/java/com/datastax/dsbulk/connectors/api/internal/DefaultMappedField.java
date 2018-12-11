/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.connectors.api.internal;

import com.datastax.dsbulk.connectors.api.MappedField;
import java.util.Objects;
import org.jetbrains.annotations.NotNull;

public class DefaultMappedField implements MappedField {

  private final String name;

  public DefaultMappedField(@NotNull String name) {
    this.name = name;
  }

  @Override
  @NotNull
  public String getFieldDescription() {
    return name;
  }

  @Override
  @NotNull
  public String getFieldName() {
    return name;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    // implementation note: it is important to consider other implementations of MappedField
    if (!(o instanceof MappedField)) {
      return false;
    }
    MappedField that = (MappedField) o;
    return name.equals(that.getFieldName());
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }

  @Override
  @NotNull
  public String toString() {
    return getFieldDescription();
  }
}
