/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.executor.api.statement;

import static com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl.literal;
import static com.datastax.oss.driver.api.querybuilder.relation.Relation.token;
import static java.util.stream.Collectors.toList;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.querybuilder.QueryBuilderDsl;
import com.datastax.oss.driver.api.querybuilder.relation.Relation;
import com.datastax.oss.driver.api.querybuilder.select.Select;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/** An utility class that helps reading all rows of a table. */
public class TableScanner {

  /**
   * Creates and returns as many {@link Statement}s as necessary to read the entire table, one per
   * token range.
   *
   * @param session a running {@link Session} to gather metadata from.
   * @param keyspace the keyspace to query.
   * @param table the table to read.
   * @return as many {@link Statement}s as necessary to read the entire table, one per token range.
   */
  public static List<Statement<?>> scan(Session session, String keyspace, String table) {
    return scan(
        session,
        session
            .getMetadata()
            .getKeyspace(keyspace)
            .orElseThrow(IllegalArgumentException::new)
            .getTable(table)
            .orElseThrow(IllegalArgumentException::new));
  }

  /**
   * Creates and returns as many {@link Statement}s as necessary to read the entire table, one per
   * token range.
   *
   * @param session a running {@link Session} to gather metadata from.
   * @param table the table to read.
   * @return as many {@link Statement}s as necessary to read the entire table, one per token range.
   */
  public static List<Statement<?>> scan(Session session, TableMetadata table) {
    return scan(
        session.getMetadata().getTokenMap().map(TokenMap::getTokenRanges).orElse(null), table);
  }

  /**
   * Creates and returns as many {@link Statement}s as necessary to read the entire table, one per
   * token range.
   *
   * @param ring the token ranges that define {@link TokenMap#getTokenRanges() data distribution} in
   *     the ring.
   * @param table the table to read.
   * @return as many {@link Statement}s as necessary to read the entire table, one per token range.
   */
  public static List<Statement<?>> scan(Set<TokenRange> ring, TableMetadata table) {
    return scan(ring, table, null);
  }

  /**
   * Creates and returns as many {@link Statement}s as necessary to read the entire table, one per
   * token range, applying an optional WHERE clause.
   *
   * @param ring the token ranges that define {@link TokenMap#getTokenRanges() data distribution} in
   *     the ring.
   * @param table the table to read.
   * @param where An optional WHERE clause to apply to each statement.
   * @return as many {@link Statement}s as necessary to read the entire table, one per token range.
   */
  public static List<Statement<?>> scan(Set<TokenRange> ring, TableMetadata table, Relation where) {
    return scan(ring, (range) -> createStatement(table, range, where));
  }

  /**
   * Creates and returns as many {@link Statement}s as necessary to read the entire table, one per
   * token range, applying an optional WHERE clause.
   *
   * @param ring the token ranges that define {@link TokenMap#getTokenRanges() data distribution} in
   *     the ring.
   * @param statementFactory a factory for statements to associate with each token range; supplied
   *     statements must have their keyspace correctly set.
   * @return as many {@link Statement}s as necessary to read the entire table, one per token range.
   */
  public static List<Statement<?>> scan(
      Set<TokenRange> ring, Function<TokenRange, Statement> statementFactory) {
    return ring.stream()
        .flatMap(
            range ->
                range
                    .unwrap()
                    .stream()
                    .map(
                        unwrapped -> {
                          Statement<?> stmt = statementFactory.apply(unwrapped);
                          return route(stmt, unwrapped, stmt.getKeyspace());
                        }))
        .collect(toList());
  }

  private static Statement<?> createStatement(TableMetadata table, TokenRange range, Relation where) {
    String[] columns =
        table.getPartitionKey().stream().map(ColumnMetadata::getName).toArray(String[]::new);
    Select stmt =
        QueryBuilderDsl.selectFrom(table.getKeyspace(), table.getName())
            .all()
            .where(
                token(columns).isGreaterThan(literal(range.getStart())),
                token(columns).isLessThanOrEqualTo(literal(range.getEnd())));
    if (where != null) {
      stmt = stmt.where(where);
    }
    return route(stmt.build(), range, table.getKeyspace());
  }

  private static <T extends Statement<T>> WrappedStatement<T> route(
      Statement<T> stmt, TokenRange range, CqlIdentifier keyspace) {
    return new WrappedStatement<T>(stmt) {

      @Override
      public CqlIdentifier getRoutingKeyspace() {
        return keyspace;
      }

      @Override
      public Token getRoutingToken() {
        return range.getEnd();
      }
    };
  }
}
