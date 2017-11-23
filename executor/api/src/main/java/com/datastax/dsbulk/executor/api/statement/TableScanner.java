/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.executor.api.statement;

import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.token;
import static java.util.stream.Collectors.toList;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.StatementWrapper;
import com.datastax.driver.core.TableMetadata;
import com.datastax.driver.core.Token;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.querybuilder.Clause;
import com.datastax.driver.core.querybuilder.Select;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

/** An utility class that helps reading all rows of a table. */
public class TableScanner {

  /**
   * Creates and returns as many {@link Statement}s as necessary to read the entire table, one per
   * token range.
   *
   * @param cluster a running {@link Cluster} to gather metadata from.
   * @param keyspace the keyspace to query.
   * @param table the table to read.
   * @return as many {@link Statement}s as necessary to read the entire table, one per token range.
   */
  public static List<Statement> scan(Cluster cluster, String keyspace, String table) {
    return scan(cluster, cluster.getMetadata().getKeyspace(keyspace).getTable(table));
  }

  /**
   * Creates and returns as many {@link Statement}s as necessary to read the entire table, one per
   * token range.
   *
   * @param cluster a running {@link Cluster} to gather metadata from.
   * @param table the table to read.
   * @return as many {@link Statement}s as necessary to read the entire table, one per token range.
   */
  public static List<Statement> scan(Cluster cluster, TableMetadata table) {
    return scan(cluster.getMetadata().getTokenRanges(), table);
  }

  /**
   * Creates and returns as many {@link Statement}s as necessary to read the entire table, one per
   * token range.
   *
   * @param ring the token ranges that define {@link Metadata#getTokenRanges() data distribution} in
   *     the ring.
   * @param table the table to read.
   * @return as many {@link Statement}s as necessary to read the entire table, one per token range.
   */
  public static List<Statement> scan(Set<TokenRange> ring, TableMetadata table) {
    return scan(ring, table, null);
  }

  /**
   * Creates and returns as many {@link Statement}s as necessary to read the entire table, one per
   * token range, applying an optional WHERE clause.
   *
   * @param ring the token ranges that define {@link Metadata#getTokenRanges() data distribution} in
   *     the ring.
   * @param table the table to read.
   * @param where An optional WHERE clause to apply to each statement.
   * @return as many {@link Statement}s as necessary to read the entire table, one per token range.
   */
  public static List<Statement> scan(Set<TokenRange> ring, TableMetadata table, Clause where) {
    return scan(ring, (range) -> createStatement(table, range, where));
  }

  /**
   * Creates and returns as many {@link Statement}s as necessary to read the entire table, one per
   * token range, applying an optional WHERE clause.
   *
   * @param ring the token ranges that define {@link Metadata#getTokenRanges() data distribution} in
   *     the ring.
   * @param statementFactory a factory for statements to associate with each token range; supplied
   *     statements must have their keyspace correctly set.
   * @return as many {@link Statement}s as necessary to read the entire table, one per token range.
   */
  public static List<Statement> scan(
      Set<TokenRange> ring, Function<TokenRange, Statement> statementFactory) {
    return ring.stream()
        .flatMap(
            range ->
                range
                    .unwrap()
                    .stream()
                    .map(
                        tr -> {
                          Statement stmt = statementFactory.apply(range);
                          return wrap(stmt, range, stmt.getKeyspace());
                        }))
        .collect(toList());
  }

  private static Statement createStatement(TableMetadata table, TokenRange range, Clause where) {
    String[] columns =
        table.getPartitionKey().stream().map(ColumnMetadata::getName).toArray(String[]::new);
    Select.Where stmt =
        select()
            .all()
            .from(table)
            .where(gt(token(columns), range.getStart()))
            .and(lte(token(columns), range.getEnd()));
    if (where != null) stmt = stmt.and(where);
    return wrap(stmt, range, table.getKeyspace().getName());
  }

  private static Statement wrap(Statement stmt, TokenRange range, String keyspace) {
    return new StatementWrapper(stmt) {
      @Override
      public Token getRoutingToken() {
        return range.getEnd();
      }

      @Override
      public String getKeyspace() {
        return keyspace;
      }
    };
  }
}
