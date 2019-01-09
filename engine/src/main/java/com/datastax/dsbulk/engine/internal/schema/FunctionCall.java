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
    cql = createCQL(keyspaceName, functionName, args);
    internal = createInternal(keyspaceName, functionName, args);
    // a function call appears in result set variables in a particular form: its internal
    // representation is considered as its CQL form itself; also, user-defined functions appear
    // keyspace-qualified in result set variable names.
    // In practice, we only care about the exact result set variable name in two cases:
    // 1) For the writetime() function, because we must apply a special treatment to it – see
    // DefaultMapping.
    // 2) For all functions under protocol V1, since in protocol V1 the server does not return
    // result set metadata; hopefully, with protocol V1 only a small set of built-in functions
    // exist, and we have tests that validate that they all work as expected.
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

  // Note: the delimiter must be ', ' with a space after the comma, since that is the way
  // C* creates variable names from function calls.

  private static String createCQL(
      @Nullable CQLIdentifier keyspaceName,
      @NotNull CQLIdentifier functionName,
      @NotNull List<CQLFragment> args) {
    if (keyspaceName == null) {
      return functionName.asCql()
          + "("
          + args.stream().map(CQLFragment::asCql).collect(Collectors.joining(", "))
          + ")";
    } else {
      return keyspaceName.asCql()
          + '.'
          + functionName.asCql()
          + "("
          + args.stream().map(CQLFragment::asCql).collect(Collectors.joining(", "))
          + ")";
    }
  }

  private static String createInternal(
      @Nullable CQLIdentifier keyspaceName,
      @NotNull CQLIdentifier functionName,
      @NotNull List<CQLFragment> args) {
    if (keyspaceName == null) {
      return functionName.asInternal()
          + "("
          + args.stream().map(CQLFragment::asInternal).collect(Collectors.joining(", "))
          + ")";
    } else {
      return keyspaceName.asInternal()
          + '.'
          + functionName.asInternal()
          + "("
          + args.stream().map(CQLFragment::asInternal).collect(Collectors.joining(", "))
          + ")";
    }
  }
}
