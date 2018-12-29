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

/** An alias for a selector in a SELECT clause. */
public class Alias implements CQLFragment {

  private final CQLFragment target;
  private final CQLIdentifier alias;

  public Alias(@NotNull CQLFragment target, @NotNull CQLIdentifier alias) {
    this.target = target;
    this.alias = alias;
  }

  @NotNull
  public CQLFragment getTarget() {
    return target;
  }

  @NotNull
  public CQLIdentifier getAlias() {
    return alias;
  }

  @Override
  @NotNull
  public String asCql() {
    return target.asCql() + " AS " + alias.asCql();
  }

  @Override
  @NotNull
  public String asVariable() {
    return alias.asVariable();
  }

  @Override
  @NotNull
  public String asInternal() {
    return target.asInternal();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Alias alias1 = (Alias) o;
    return target.equals(alias1.target) && alias.equals(alias1.alias);
  }

  @Override
  public int hashCode() {
    return Objects.hash(target, alias);
  }

  @Override
  public String toString() {
    return asCql();
  }
}
