/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import com.datastax.dsbulk.commons.internal.utils.StringUtils;
import com.google.common.base.Preconditions;
import org.jetbrains.annotations.NotNull;

/**
 * The identifier of CQL element (keyspace, table, column, named bound variable, etc).
 *
 * <p>It has two representations:
 *
 * <ul>
 *   <li>the "CQL" form, which is how you would type the identifier in a CQL query. It is
 *       case-insensitive unless enclosed in double quotation marks; in addition, identifiers that
 *       contain special characters (anything other than alphanumeric and underscore), or match CQL
 *       keywords, must be double-quoted (with inner double quotes escaped as {@code ""}).
 *   <li>the "internal" form, which is how the name is stored in Cassandra system tables. It is
 *       lower-case for case-sensitive identifiers, and in the exact case for case-sensitive
 *       identifiers.
 * </ul>
 *
 * Examples:
 *
 * <table summary="examples">
 *   <tr><th>Create statement</th><th>Case-sensitive?</th><th>CQL id</th><th>Internal id</th></tr>
 *   <tr><td>CREATE TABLE t(foo int PRIMARY KEY)</td><td>No</td><td>foo</td><td>foo</td></tr>
 *   <tr><td>CREATE TABLE t(Foo int PRIMARY KEY)</td><td>No</td><td>foo</td><td>foo</td></tr>
 *   <tr><td>CREATE TABLE t("Foo" int PRIMARY KEY)</td><td>Yes</td><td>"Foo"</td><td>Foo</td></tr>
 *   <tr><td>CREATE TABLE t("foo bar" int PRIMARY KEY)</td><td>Yes</td><td>"foo bar"</td><td>foo bar</td></tr>
 *   <tr><td>CREATE TABLE t("foo""bar" int PRIMARY KEY)</td><td>Yes</td><td>"foo""bar"</td><td>foo"bar</td></tr>
 *   <tr><td>CREATE TABLE t("create" int PRIMARY KEY)</td><td>Yes (reserved keyword)</td><td>"create"</td><td>create</td></tr>
 * </table>
 *
 * This class provides a common representation and avoids any ambiguity about which form the
 * identifier is in. Driver clients will generally want to create instances from the CQL form with
 * {@link #fromCql(String)}.
 *
 * <p>There is no internal caching; if you reuse the same identifiers often,
 */
public class CQLIdentifier implements CQLFragment {

  /** Creates an identifier from its {@link CQLIdentifier CQL form}. */
  @NotNull
  public static CQLIdentifier fromCql(@NotNull String cql) {
    Preconditions.checkNotNull(cql, "cql must not be null");
    final String internal;
    if (StringUtils.isDoubleQuoted(cql)) {
      internal = StringUtils.unDoubleQuote(cql);
    } else {
      internal = cql.toLowerCase();
      Preconditions.checkArgument(
          !StringUtils.needsDoubleQuotes(internal),
          "Invalid CQL form [%s]: needs double quotes",
          cql);
    }
    return fromInternal(internal);
  }

  /** Creates an identifier from its {@link CQLIdentifier internal form}. */
  @NotNull
  public static CQLIdentifier fromInternal(@NotNull String internal) {
    Preconditions.checkNotNull(internal, "internal must not be null");
    return new CQLIdentifier(internal);
  }

  private final String internal;
  private final String variable;
  private final String assignment;

  private CQLIdentifier(String internal) {
    this.internal = internal;
    variable =
        StringUtils.needsDoubleQuotes(internal) ? StringUtils.doubleQuote(internal) : internal;
    assignment = ':' + variable;
  }

  @Override
  public String render(CQLRenderMode mode) {
    switch (mode) {
      case NAMED_ASSIGNMENT:
        return assignment;
      case POSITIONAL_ASSIGNMENT:
        return "?";
      case ALIASED_SELECTOR:
      case UNALIASED_SELECTOR:
      case VARIABLE:
        return variable;
      case INTERNAL:
      default:
        return internal;
    }
  }

  @Override
  public boolean equals(Object other) {
    if (other == this) {
      return true;
    } else if (other instanceof CQLIdentifier) {
      CQLIdentifier that = (CQLIdentifier) other;
      return this.internal.equals(that.internal);
    } else {
      return false;
    }
  }

  @Override
  public int hashCode() {
    return internal.hashCode();
  }

  @Override
  public String toString() {
    return internal;
  }
}
