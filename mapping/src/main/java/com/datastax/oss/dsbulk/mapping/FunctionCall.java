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

import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import edu.umd.cs.findbugs.annotations.NonNull;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collector;
import java.util.stream.Collectors;

/**
 * A CQL function call as declared in a query or in a mapping entry.
 *
 * <p>In mapping declarations, function calls can appear in both sides of a mapping entry, which is
 * why this class implements both {@link MappingField} (left side) and {@link CQLFragment} (right
 * side).
 */
public class FunctionCall implements MappingField, CQLFragment {

  /**
   * Note: the delimiter must be ', ' with a space after the comma, since that is the way C* creates
   * variable names from function calls.
   */
  private static final Collector<CharSequence, ?, String> COMMA = Collectors.joining(", ");

  private final CQLWord keyspaceName;
  private final CQLWord functionName;
  private final ImmutableList<CQLFragment> args;

  private final String namedAssignment;
  private final String positionalAssignment;
  private final String internal;
  private final String identifier;
  private final String unaliasedSelector;
  private final String aliasedSelector;

  public FunctionCall(
      @Nullable CQLWord keyspaceName, @NonNull CQLWord functionName, @NonNull CQLFragment... args) {
    this(keyspaceName, functionName, Arrays.asList(args));
  }

  public FunctionCall(
      @Nullable CQLWord keyspaceName,
      @NonNull CQLWord functionName,
      @NonNull List<CQLFragment> args) {
    this.keyspaceName = keyspaceName;
    this.functionName = functionName;
    this.args = ImmutableList.copyOf(args);
    namedAssignment = renderNamedAssignment();
    positionalAssignment = renderPositionalAssignment();
    internal = renderInternal();
    identifier = CQLWord.fromInternal(internal).render(CQLRenderMode.VARIABLE);
    unaliasedSelector = renderUnaliasedSelector();
    aliasedSelector = renderAliasedSelector();
  }

  private String renderNamedAssignment() {
    String name = functionName.render(CQLRenderMode.VARIABLE);
    String argsList =
        args.stream().map(arg -> arg.render(CQLRenderMode.NAMED_ASSIGNMENT)).collect(COMMA);
    if (keyspaceName == null) {
      return String.format("%s(%s)", name, argsList);
    } else {
      return String.format(
          "%s.%s(%s)", keyspaceName.render(CQLRenderMode.VARIABLE), name, argsList);
    }
  }

  private String renderPositionalAssignment() {
    String name = functionName.render(CQLRenderMode.VARIABLE);
    String argsList =
        args.stream().map(arg -> arg.render(CQLRenderMode.POSITIONAL_ASSIGNMENT)).collect(COMMA);
    if (keyspaceName == null) {
      return String.format("%s(%s)", name, argsList);
    } else {
      return String.format(
          "%s.%s(%s)", keyspaceName.render(CQLRenderMode.VARIABLE), name, argsList);
    }
  }

  private String renderInternal() {
    String name = functionName.render(CQLRenderMode.INTERNAL);
    String argsList = args.stream().map(arg -> arg.render(CQLRenderMode.INTERNAL)).collect(COMMA);
    if (keyspaceName == null) {
      return String.format("%s(%s)", name, argsList);
    } else {
      return String.format(
          "%s.%s(%s)", keyspaceName.render(CQLRenderMode.INTERNAL), name, argsList);
    }
  }

  private String renderUnaliasedSelector() {
    String name = functionName.render(CQLRenderMode.VARIABLE);
    String argsList =
        args.stream().map(arg -> arg.render(CQLRenderMode.UNALIASED_SELECTOR)).collect(COMMA);
    if (keyspaceName == null) {
      return String.format("%s(%s)", name, argsList);
    } else {
      return String.format(
          "%s.%s(%s)", keyspaceName.render(CQLRenderMode.VARIABLE), name, argsList);
    }
  }

  private String renderAliasedSelector() {
    return String.format("%s AS %s", unaliasedSelector, identifier);
  }

  @NonNull
  public Optional<CQLWord> getKeyspaceName() {
    return Optional.ofNullable(keyspaceName);
  }

  @NonNull
  public CQLWord getFunctionName() {
    return functionName;
  }

  @Override
  @NonNull
  public String getFieldDescription() {
    return render(CQLRenderMode.INTERNAL);
  }

  @NonNull
  public ImmutableList<CQLFragment> getArgs() {
    return args;
  }

  @Override
  public String render(CQLRenderMode mode) {
    switch (mode) {
      case NAMED_ASSIGNMENT:
        return namedAssignment;
      case POSITIONAL_ASSIGNMENT:
        return positionalAssignment;
      case UNALIASED_SELECTOR:
        return unaliasedSelector;
      case ALIASED_SELECTOR:
        return aliasedSelector;
      case VARIABLE:
        return identifier;
      case INTERNAL:
      default:
        return internal;
    }
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof FunctionCall)) {
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
    return getFieldDescription();
  }
}
