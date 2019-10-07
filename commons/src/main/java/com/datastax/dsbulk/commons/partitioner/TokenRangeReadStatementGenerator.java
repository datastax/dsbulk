/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.partitioner;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.RelationMetadata;
import com.datastax.oss.driver.internal.core.metadata.token.DefaultTokenMap;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Generates SELECT statements that read the entire table by token ranges. */
public class TokenRangeReadStatementGenerator {

  private final RelationMetadata table;
  private final TokenMap tokenMap;

  /**
   * @param table The table to scan.
   * @param metadata The cluster metadata to use.
   */
  public TokenRangeReadStatementGenerator(RelationMetadata table, Metadata metadata) {
    this.table = table;
    tokenMap =
        metadata
            .getTokenMap()
            .orElseThrow(() -> new IllegalStateException("Token metadata not present"));
  }

  /**
   * Generates default SELECT statements to read the entire table, with a minimum of {@code
   * splitCount} statements.
   *
   * <p>For a given split / token range, the generated statement is a {@linkplain Statement
   * statement} of the form: {@code SELECT col1, col2,... FROM table WHERE token(...) >
   * [range.start()] AND token(...) <= [range.end()])}.
   *
   * <p>Note that the splitting algorithm doesn't guarantee an exact number of splits, but rather a
   * minimum number. The number of resulting statements depends on the set of primary token ranges
   * in the ring and how contiguous token ranges are distributed across the ring. In particular with
   * vnodes, the total number of statements can be much higher than {@code splitCount}.
   *
   * @param splitCount The minimum desired number of statements to generate (on a best-effort
   *     basis).
   * @return A list of SELECT statements to read the entire table.
   */
  public List<Statement<?>> generate(int splitCount) {
    return generate(splitCount, this::generateSimpleStatement);
  }

  /**
   * Generates SELECT statements to read the entire table, with a minimum of {@code splitCount}
   * statements and using the given factory to generate statements.
   *
   * <p>For each split / token range, the generated statement is a {@linkplain Statement statement}
   * resulting from applying {@code statementFactory} to the token range; statement factories should
   * typically generate a statement of the form: {@code SELECT col1, col2,... FROM table WHERE
   * token(...) > ? AND token(...) <= ?)}. Please note that this method does not fully validate that
   * the statements created by the factory are valid, and thus should be used with caution.
   *
   * <p>Note that the splitting algorithm doesn't guarantee an exact number of splits, but rather a
   * minimum number. The number of resulting statements depends on the set of primary token ranges
   * in the ring and how contiguous token ranges are distributed across the ring. In particular with
   * vnodes, the total number of statements can be much higher than {@code splitCount}.
   *
   * @param splitCount The minimum desired number of statements to generate (on a best-effort
   *     basis).
   * @param statementFactory The factory to use to generate statements for each split.
   * @return A list of SELECT statements to read the entire table.
   */
  public List<Statement<?>> generate(
      int splitCount, Function<TokenRange<?, ?>, Statement<?>> statementFactory) {
    @SuppressWarnings("unchecked")
    TokenFactory<Number, Token<Number>> tokenFactory =
        (TokenFactory<Number, Token<Number>>)
            TokenFactory.forDriverTokenFactory(((DefaultTokenMap) tokenMap).getTokenFactory());
    PartitionGenerator<Number, Token<Number>> generator =
        new PartitionGenerator<>(table.getKeyspace(), tokenMap, tokenFactory);
    List<TokenRange<Number, Token<Number>>> partitions = generator.partition(splitCount);
    List<Statement<?>> statements = new ArrayList<>();
    for (TokenRange<Number, Token<Number>> range : partitions) {
      Statement<?> stmt = statementFactory.apply(range);
      if (stmt.getKeyspace() != null) {
        if (!stmt.getKeyspace().equals(table.getKeyspace())) {
          throw new IllegalStateException(
              String.format(
                  "Statement has different keyspace, expecting %s but got %s",
                  table.getKeyspace(), stmt.getKeyspace()));
        }
      } else {
        stmt = stmt.setRoutingKeyspace(table.getKeyspace());
      }
      stmt = stmt.setRoutingToken(tokenMap.parse(range.end().toString()));
      statements.add(stmt);
    }
    return statements;
  }

  private Statement<?> generateSimpleStatement(TokenRange<?, ?> range) {
    String all =
        table.getColumns().keySet().stream()
            .map(id -> id.asCql(true))
            .collect(Collectors.joining(","));
    String pks =
        table.getPartitionKey().stream()
            .map(ColumnMetadata::getName)
            .map(id -> id.asCql(true))
            .collect(Collectors.joining(","));
    String query =
        String.format(
            "SELECT %s FROM %s.%s WHERE token(%s) > %s AND token(%s) <= %s",
            all,
            table.getKeyspace().asCql(true),
            table.getName().asCql(true),
            pks,
            range.start().value(),
            pks,
            range.end().value());
    return SimpleStatement.newInstance(query);
  }
}
