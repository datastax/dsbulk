/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;

/**
 * A CQL function call as declared in a query or in a mapping entry.
 *
 * <p>In mapping declarations, function calls can appear in both sides of a mapping entry, which is
 * why this class implements both {@link MappingField} (left side) and {@link CQLFragment} (right
 * side).
 */
public class FunctionCall implements MappingField, CQLFragment {

  private final CQLIdentifier name;
  private final ImmutableList<CQLFragment> args;
  private final String cql;
  private final String variable;
  private final String internal;

  public FunctionCall(@NotNull CQLIdentifier name, @NotNull CQLFragment... args) {
    this(name, Arrays.asList(args));
  }

  public FunctionCall(@NotNull CQLIdentifier name, @NotNull List<CQLFragment> args) {
    this.name = name;
    this.args = ImmutableList.copyOf(args);
    // Note: the delimiter must be ', ' with a space after the comma, since that is the way
    // C* creates variable names from function calls.
    cql =
        name.asCql()
            + "("
            + args.stream().map(CQLFragment::asCql).collect(Collectors.joining(", "))
            + ")";
    internal =
        name.asInternal()
            + "("
            + args.stream().map(CQLFragment::asInternal).collect(Collectors.joining(", "))
            + ")";
    // a function call appears in result set variables in a particular form: its internal
    // representation is considered as its CQL form itself.
    variable = CQLIdentifier.fromInternal(internal).asCql();
  }

  @NotNull
  public CQLIdentifier getFunctionName() {
    return name;
  }

  @Override
  @NotNull
  public String getFieldDescription() {
    return getFunctionName().asCql();
  }

  @NotNull
  public ImmutableList<CQLFragment> getArgs() {
    return args;
  }

  @Override
  @NotNull
  public String asCql() {
    return cql;
  }

  @Override
  @NotNull
  public String asVariable() {
    return variable;
  }

  @Override
  @NotNull
  public String asInternal() {
    return internal;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FunctionCall that = (FunctionCall) o;
    return name.equals(that.name) && args.equals(that.args);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, args);
  }

  @Override
  public String toString() {
    return asCql();
  }
}
