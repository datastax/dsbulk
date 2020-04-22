/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.connectors.api;

import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.Objects;

public class DefaultMappedField implements MappedField {

  private final String name;

  public DefaultMappedField(@NonNull String name) {
    this.name = name;
  }

  @Override
  @NonNull
  public String getFieldDescription() {
    return name;
  }

  @Override
  @NonNull
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
  @NonNull
  public String toString() {
    return getFieldDescription();
  }
}
