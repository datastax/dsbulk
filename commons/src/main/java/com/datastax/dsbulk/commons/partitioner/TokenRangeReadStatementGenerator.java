/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.partitioner;

import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lte;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.token;

import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.ColumnMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/** Generates SELECT statements that read the entire table by token ranges. */
public class TokenRangeReadStatementGenerator {

  private final AbstractTableMetadata table;
  private final Metadata metadata;

  /**
   * @param table The table to scan.
   * @param metadata The cluster metadata to use.
   */
  public TokenRangeReadStatementGenerator(AbstractTableMetadata table, Metadata metadata) {
    this.table = table;
    this.metadata = metadata;
  }

  /**
   * Generates default SELECT statements to read the entire table, with a minimum of {@code
   * splitCount} statements.
   *
   * <p>For a given split / token range, the generated statement is a {@linkplain
   * TokenRangeReadStatement wrapper} wrapping a {@link BuiltStatement} of the form: {@code SELECT
   * col1, col2,... FROM table WHERE token(...) > [range.start()] AND token(...) <= [range.end()])}.
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
  public List<TokenRangeReadStatement> generate(int splitCount) {
    return generate(splitCount, this::buildWrapped);
  }

  /**
   * Generates SELECT statements to read the entire table, with a minimum of {@code splitCount}
   * statements and using the given factory to generate statements.
   *
   * <p>For each split / token range, the generated statement is a {@linkplain
   * TokenRangeReadStatement wrapper} wrapping the result of applying {@code statementFactory} to
   * the token range; statement factories should typically generate a statement of the form: {@code
   * SELECT col1, col2,... FROM table WHERE token(...) > ? AND token(...) <= ?)}.
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
  public List<TokenRangeReadStatement> generate(
      int splitCount, Function<TokenRange<?, ?>, Statement> statementFactory) {
    @SuppressWarnings("unchecked")
    TokenFactory<Number, Token<Number>> tokenFactory =
        (TokenFactory<Number, Token<Number>>)
            TokenFactory.forPartitioner(metadata.getPartitioner());
    PartitionGenerator<Number, Token<Number>> generator =
        new PartitionGenerator<>(table.getKeyspace(), metadata, tokenFactory);
    List<TokenRange<Number, Token<Number>>> partitions = generator.partition(splitCount);
    List<TokenRangeReadStatement> statements = new ArrayList<>();
    for (TokenRange<Number, Token<Number>> range : partitions) {
      TokenRangeReadStatement stmt =
          new TokenRangeReadStatement(
              statementFactory.apply(range),
              table,
              range,
              metadata.newToken(range.end().toString()));
      statements.add(stmt);
    }
    return statements;
  }

  private BuiltStatement buildWrapped(TokenRange<?, ?> range) {
    String[] pks =
        table
            .getPartitionKey()
            .stream()
            .map(ColumnMetadata::getName)
            .map(Metadata::quoteIfNecessary)
            .toArray(String[]::new);
    String[] all =
        table
            .getColumns()
            .stream()
            .map(ColumnMetadata::getName)
            .map(Metadata::quoteIfNecessary)
            .toArray(String[]::new);
    return select(all)
        .from(
            Metadata.quoteIfNecessary(table.getKeyspace().getName()),
            Metadata.quoteIfNecessary(table.getName()))
        .where(gt(token(pks), range.start().value()))
        .and(lte(token(pks), range.end().value()));
  }
}
