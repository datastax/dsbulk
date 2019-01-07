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
import java.util.Optional;
import java.util.stream.Collectors;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

/**
 * A CQL function call as declared in a query or in a mapping entry.
 *
 * <p>In mapping declarations, function calls can appear in both sides of a mapping entry, which is
 * why this class implements both {@link MappingField} (left side) and {@link CQLFragment} (right
 * side).
 */
public class FunctionCall implements MappingField, CQLFragment {

  private final CQLIdentifier keyspaceName;
  private final CQLIdentifier functionName;
  private final ImmutableList<CQLFragment> args;
  private final String cql;
  private final String variable;
  private final String internal;

  public FunctionCall(
      @Nullable CQLIdentifier keyspaceName,
      @NotNull CQLIdentifier functionName,
      @NotNull CQLFragment... args) {
    this(keyspaceName, functionName, Arrays.asList(args));
  }

  public FunctionCall(
      @Nullable CQLIdentifier keyspaceName,
      @NotNull CQLIdentifier functionName,
      @NotNull List<CQLFragment> args) {
    this.keyspaceName = keyspaceName;
    this.functionName = functionName;
    this.args = ImmutableList.copyOf(args);
    // Note: the delimiter must be ', ' with a space after the comma, since that is the way
    // C* creates variable names from function calls.
    if (keyspaceName == null) {
      cql =
          functionName.asCql()
              + "("
              + args.stream().map(CQLFragment::asCql).collect(Collectors.joining(", "))
              + ")";
      internal =
          functionName.asInternal()
              + "("
              + args.stream().map(CQLFragment::asInternal).collect(Collectors.joining(", "))
              + ")";
    } else {
      cql =
          keyspaceName.asCql()
              + '.'
              + functionName.asCql()
              + "("
              + args.stream().map(CQLFragment::asCql).collect(Collectors.joining(", "))
              + ")";
      internal =
          keyspaceName.asInternal()
              + '.'
              + functionName.asInternal()
              + "("
              + args.stream().map(CQLFragment::asInternal).collect(Collectors.joining(", "))
              + ")";
    }
    // a function call appears in result set variables in a particular form: its internal
    // representation is considered as its CQL form itself.
    // Note: this is only true for built-in functions, such as now(), ttl(), writetime() or token();
    // user-defined functions appear keyspace-qualified in result set variable names.
    // In practice, this will never be a problem since we only care about built-in functions
    // and specially writetime() – see DefaultMapping.
    variable = CQLIdentifier.fromInternal(internal).asCql();
  }

  @NotNull
  public Optional<CQLIdentifier> getKeyspaceName() {
    return Optional.ofNullable(keyspaceName);
  }

  @NotNull
  public CQLIdentifier getFunctionName() {
    return functionName;
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
    return Objects.equals(keyspaceName, that.keyspaceName)
        && functionName.equals(that.functionName)
        && args.equals(that.args);
  }

  @Override
  public int hashCode() {
    return Objects.hash(keyspaceName, functionName, args);
  }

  @Override
  public String toString() {
    return asCql();
  }
}
