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
package com.datastax.oss.dsbulk.mapping;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import edu.umd.cs.findbugs.annotations.NonNull;

/**
 * A CQL identifier (keyspace, table, column, named bound variable, etc).
 *
 * <p>This class is merely a wrapper around the driver's {@link CqlIdentifier} that also implements
 * {@link CQLFragment}.
 *
 * @see CqlIdentifier
 */
public class CQLWord implements CQLFragment {

  /** Creates an identifier from its {@link CQLWord CQL form}. */
  @NonNull
  public static CQLWord fromCql(@NonNull String cql) {
    return fromCqlIdentifier(CqlIdentifier.fromCql(cql));
  }

  /** Creates an identifier from its {@link CQLWord internal form}. */
  @NonNull
  public static CQLWord fromInternal(@NonNull String internal) {
    return fromCqlIdentifier(CqlIdentifier.fromInternal(internal));
  }

  /** Creates an identifier from a {@link CqlIdentifier}. */
  @NonNull
  public static CQLWord fromCqlIdentifier(@NonNull CqlIdentifier identifier) {
    return new CQLWord(identifier);
  }

  private final CqlIdentifier identifier;

  private CQLWord(@NonNull CqlIdentifier identifier) {
    this.identifier = identifier;
  }

  public CqlIdentifier asIdentifier() {
    return identifier;
  }

  @Override
  public String render(CQLRenderMode mode) {
    switch (mode) {
      case NAMED_ASSIGNMENT:
        return ':' + identifier.asCql(true);
      case POSITIONAL_ASSIGNMENT:
        return "?";
      case ALIASED_SELECTOR:
      case UNALIASED_SELECTOR:
      case VARIABLE:
        return identifier.asCql(true);
      case INTERNAL:
      default:
        return identifier.asInternal();
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof CQLWord) {
      CQLWord that = (CQLWord) other;
      return this.identifier.equals(that.identifier);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return identifier.hashCode();
  }

  @Override
  public String toString() {
    return identifier.asInternal();
  }
}
