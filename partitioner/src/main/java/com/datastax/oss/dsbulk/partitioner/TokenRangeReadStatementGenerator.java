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
package com.datastax.oss.dsbulk.partitioner;

import static com.datastax.oss.dsbulk.partitioner.utils.TokenUtils.getTokenValue;

import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.metadata.Metadata;
import com.datastax.oss.driver.api.core.metadata.TokenMap;
import com.datastax.oss.driver.api.core.metadata.schema.ColumnMetadata;
import com.datastax.oss.driver.api.core.metadata.schema.RelationMetadata;
import com.datastax.oss.driver.api.core.metadata.token.TokenRange;
import com.datastax.oss.driver.internal.core.metadata.token.DefaultTokenMap;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Generates SELECT statements that read the entire table by token ranges. */
public class TokenRangeReadStatementGenerator {

  private final RelationMetadata table;
  private final TokenMap tokenMap;

  /**
   * @param table The table (or materialized view) to scan.
   * @param metadata The cluster metadata to use.
   */
  public TokenRangeReadStatementGenerator(
      @NonNull RelationMetadata table, @NonNull Metadata metadata) {
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
  @NonNull
  public Map<TokenRange, SimpleStatement> generate(int splitCount) {
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
  @NonNull
  public <StatementT extends Statement<StatementT>> Map<TokenRange, StatementT> generate(
      int splitCount, @NonNull Function<TokenRange, StatementT> statementFactory) {
    BulkTokenFactory tokenFactory =
        BulkTokenFactory.forPartitioner(
            ((DefaultTokenMap) tokenMap).getTokenFactory().getPartitionerName());
    PartitionGenerator generator =
        new PartitionGenerator(table.getKeyspace(), tokenMap, tokenFactory);
    List<BulkTokenRange> partitions = generator.partition(splitCount);
    Map<TokenRange, StatementT> statements = new TreeMap<>();
    for (BulkTokenRange range : partitions) {
      StatementT stmt = statementFactory.apply(range);
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
      stmt = stmt.setRoutingToken(range.getEnd());
      statements.put(range, stmt);
    }
    return statements;
  }

  private SimpleStatement generateSimpleStatement(TokenRange range) {
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
            getTokenValue(range.getStart()),
            pks,
            getTokenValue(range.getEnd()));
    return SimpleStatement.newInstance(query);
  }
}
