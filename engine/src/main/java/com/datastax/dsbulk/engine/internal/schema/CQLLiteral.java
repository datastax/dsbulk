/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import java.util.Objects;
import org.jetbrains.annotations.NotNull;

/** A CQL literal, such as an integer, float, UUID, hex number, boolean, or string literal. */
public class CQLLiteral implements CQLFragment {

  private final String literal;

  public CQLLiteral(String literal) {
    this.literal = literal;
  }

  public String getLiteral() {
    return literal;
  }

  @Override
  public @NotNull String asCql() {
    return literal;
  }

  @Override
  public @NotNull String asInternal() {
    return literal;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CQLLiteral that = (CQLLiteral) o;
    return literal.equals(that.literal);
  }

  @Override
  public int hashCode() {
    return Objects.hash(literal);
  }

  @Override
  public String toString() {
    return literal;
  }
}
