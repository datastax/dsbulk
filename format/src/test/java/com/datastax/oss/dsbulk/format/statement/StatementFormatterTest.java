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
package com.datastax.oss.dsbulk.format.statement;

import static com.datastax.oss.driver.shaded.guava.common.base.Strings.repeat;
import static com.datastax.oss.dsbulk.tests.driver.DriverUtils.UNSET;
import static com.datastax.oss.dsbulk.tests.driver.DriverUtils.mockBoundStatement;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.oss.driver.api.core.DefaultConsistencyLevel;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.DefaultBatchType;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.cql.Statement;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableMap;
import com.datastax.oss.dsbulk.tests.DataTypesProvider;
import com.datastax.oss.protocol.internal.util.Bytes;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

class StatementFormatterTest {

  private final ProtocolVersion version = ProtocolVersion.DEFAULT;

  private final CodecRegistry codecRegistry = CodecRegistry.DEFAULT;

  // Basic Tests

  @Test
  void should_format_simple_statement() {
    StatementFormatter formatter = StatementFormatter.builder().build();
    Statement<?> statement =
        SimpleStatement.newInstance("SELECT * FROM t WHERE c1 = ? AND c2 = ?", "foo", 42)
            .setIdempotent(true)
            .setConsistencyLevel(DefaultConsistencyLevel.QUORUM)
            .setSerialConsistencyLevel(DefaultConsistencyLevel.SERIAL)
            .setQueryTimestamp(12345L)
            .setTimeout(Duration.ofMillis(12345L));
    String s =
        formatter.format(statement, StatementFormatVerbosity.EXTENDED, version, codecRegistry);
    assertThat(s)
        .contains("SimpleStatement@")
        .contains(
            "idempotence: true, CL: QUORUM, serial CL: SERIAL, timestamp: 12345, timeout: PT12.345S")
        .contains("2 values")
        .contains("SELECT * FROM t WHERE c1 = ? AND c2 = ?")
        .contains("0: 'foo'")
        .contains("1: 42");
  }

  @Test
  void should_use_toString_when_exception_during_formatting() {
    StatementFormatter formatter = StatementFormatter.builder().build();
    SimpleStatement statement = mock(SimpleStatement.class);
    when(statement.getPositionalValues()).thenThrow(new NullPointerException());
    assertThat(
            formatter.format(statement, StatementFormatVerbosity.EXTENDED, version, codecRegistry))
        .isEqualTo(statement.toString());
  }

  @Test
  void should_use_generic_description_exception_during_formatting_and_toString() {
    StatementFormatter formatter = StatementFormatter.builder().build();
    SimpleStatement statement = mock(SimpleStatement.class);
    when(statement.getPositionalValues()).thenThrow(new NullPointerException());
    when(statement.toString()).thenThrow(new NullPointerException());
    assertThat(
            formatter.format(statement, StatementFormatVerbosity.EXTENDED, version, codecRegistry))
        .isEqualTo("statement[?]");
  }

  @Test
  void should_format_simple_statement_with_named_values() {
    StatementFormatter formatter = StatementFormatter.builder().build();
    Statement<?> statement =
        SimpleStatement.newInstance(
            "SELECT * FROM t WHERE c1 = ? AND c2 = ?", ImmutableMap.of("c1", "foo", "c2", 42));
    String s =
        formatter.format(statement, StatementFormatVerbosity.EXTENDED, version, codecRegistry);
    assertThat(s)
        .contains("SimpleStatement@")
        .contains("2 values")
        .contains("SELECT * FROM t WHERE c1 = ? AND c2 = ?")
        .contains("c1: 'foo'")
        .contains("c2: 42");
  }

  @Test
  void should_format_statement_without_values() {
    StatementFormatter formatter = StatementFormatter.builder().build();
    Statement<?> statement = SimpleStatement.newInstance("SELECT * FROM t WHERE c1 = 42");
    String s =
        formatter.format(statement, StatementFormatVerbosity.EXTENDED, version, codecRegistry);
    assertThat(s)
        .contains("SimpleStatement@")
        .contains("0 values")
        .contains("SELECT * FROM t WHERE c1 = 42")
        .doesNotContain("{");
  }

  @Test
  void should_format_bound_statement() {
    StatementFormatter formatter = StatementFormatter.builder().build();
    BoundStatement statement =
        mockBoundStatement(
            "SELECT * FROM t WHERE c1 = ? AND c2 = ? AND c3 = ?", "foo", null, UNSET);
    String s =
        formatter.format(statement, StatementFormatVerbosity.EXTENDED, version, codecRegistry);
    assertThat(s)
        .contains("BoundStatement")
        .contains("3 values")
        .contains("SELECT * FROM t WHERE c1 = ? AND c2 = ? AND c3 = ?")
        .contains("c1: 'foo'")
        .contains("c2: <NULL>")
        .contains("c3: <UNSET>");
  }

  @Test
  void should_format_batch_statement() {
    StatementFormatter formatter = StatementFormatter.builder().build();
    BatchableStatement<?> inner1 =
        mockBoundStatement("SELECT * FROM t WHERE c1 = ? AND c2 = ?", "foo", null, UNSET);
    BatchableStatement<?> inner2 =
        SimpleStatement.newInstance("SELECT * FROM t WHERE c1 = ? AND c2 = ?", "foo", 42);
    BatchStatement statement =
        BatchStatement.newInstance(DefaultBatchType.UNLOGGED, inner1, inner2);
    String s =
        formatter.format(statement, StatementFormatVerbosity.EXTENDED, version, codecRegistry);
    assertThat(s.replaceAll("\\s", ""))
        .contains("BatchStatement@")
        .contains("[UNLOGGED,2stmts,5values]")
        .contains(
            "1:"
                + formatter
                    .format(inner1, StatementFormatVerbosity.EXTENDED, version, codecRegistry)
                    .replaceAll("\\s", ""))
        .contains(
            "2:"
                + formatter
                    .format(inner2, StatementFormatVerbosity.EXTENDED, version, codecRegistry)
                    .replaceAll("\\s", ""));
  }

  // Verbosity

  @Test
  void should_format_with_abridged_verbosity() {
    StatementFormatter formatter = StatementFormatter.builder().build();
    Statement<?> statement =
        SimpleStatement.newInstance("SELECT * FROM t WHERE c1 = ? AND c2 = ?", "foo", 42);
    String s =
        formatter.format(statement, StatementFormatVerbosity.ABRIDGED, version, codecRegistry);
    assertThat(s)
        .contains("SimpleStatement@")
        .doesNotContain("idempotence")
        .doesNotContain("CL")
        .doesNotContain("serial CL")
        .doesNotContain("timeout")
        .doesNotContain("timestamp")
        .contains("2 values")
        .doesNotContain("SELECT * FROM t WHERE c1 = ? AND c2 = ?")
        .doesNotContain("{ 0: 'foo', 1: 42 }");
  }

  @Test
  void should_format_with_normal_verbosity() {
    StatementFormatter formatter = StatementFormatter.builder().build();
    Statement<?> statement =
        SimpleStatement.newInstance("SELECT * FROM t WHERE c1 = ? AND c2 = ?", "foo", 42);
    String s = formatter.format(statement, StatementFormatVerbosity.NORMAL, version, codecRegistry);
    assertThat(s)
        .contains("SimpleStatement@")
        .doesNotContain("idempotence")
        .doesNotContain("CL")
        .doesNotContain("serial CL")
        .doesNotContain("serial CL")
        .doesNotContain("timeout")
        .doesNotContain("timestamp")
        .contains("2 values")
        .contains("SELECT * FROM t WHERE c1 = ? AND c2 = ?")
        .doesNotContain("{ 0: 'foo', 1: 42 }");
  }

  // Limits

  @Test
  void should_truncate_query_string() {
    StatementFormatter formatter = StatementFormatter.builder().withMaxQueryStringLength(7).build();
    SimpleStatement statement = SimpleStatement.newInstance("123456789");
    String s =
        formatter.format(statement, StatementFormatVerbosity.EXTENDED, version, codecRegistry);
    assertThat(s).contains("1234567...");
  }

  @Test
  void should_not_truncate_query_string_when_unlimited() {
    StatementFormatter formatter =
        StatementFormatter.builder()
            .withMaxQueryStringLength(StatementFormatterLimits.UNLIMITED)
            .build();
    String query = repeat("a", 5000);
    SimpleStatement statement = SimpleStatement.newInstance(query);
    String s =
        formatter.format(statement, StatementFormatVerbosity.EXTENDED, version, codecRegistry);
    assertThat(s).contains(query);
  }

  @Test
  void should_not_print_more_bound_values_than_max() {
    StatementFormatter formatter = StatementFormatter.builder().withMaxBoundValues(2).build();
    SimpleStatement statement = SimpleStatement.newInstance("query", 0, 1, 2, 3);
    String s =
        formatter.format(statement, StatementFormatVerbosity.EXTENDED, version, codecRegistry);
    assertThat(s).contains("0: 0").contains("1: 1").contains("...");
  }

  @Test
  void should_truncate_bound_value() {
    StatementFormatter formatter = StatementFormatter.builder().withMaxBoundValueLength(4).build();
    SimpleStatement statement = SimpleStatement.newInstance("query", "12345");
    String s =
        formatter.format(statement, StatementFormatVerbosity.EXTENDED, version, codecRegistry);
    assertThat(s).contains("0: '123...");
  }

  @Test
  void should_truncate_bound_value_byte_buffer() {
    StatementFormatter formatter = StatementFormatter.builder().withMaxBoundValueLength(4).build();
    SimpleStatement statement =
        SimpleStatement.newInstance("query", Bytes.fromHexString("0xCAFEBABE"));
    String s =
        formatter.format(statement, StatementFormatVerbosity.EXTENDED, version, codecRegistry);
    assertThat(s).contains("0: 0xca...");
  }

  @Test
  void should_truncate_inner_statements() {
    StatementFormatter formatter =
        StatementFormatter.builder().withMaxInnerStatements(2).withMaxBoundValues(2).build();
    BatchableStatement<?> inner1 =
        mockBoundStatement("SELECT * FROM t WHERE c1 = ? AND c2 = ?", "foo", null, UNSET);
    BatchableStatement<?> inner2 =
        SimpleStatement.newInstance("SELECT * FROM t WHERE c1 = ? AND c2 = ?", "foo", 42);
    BatchableStatement<?> inner3 =
        SimpleStatement.newInstance(
            "SELECT * FROM t WHERE c1 = ? AND c2 = ? AND c3 = ?", "foo", 42, "bar");
    BatchStatement statement =
        BatchStatement.newInstance(DefaultBatchType.UNLOGGED, inner1, inner2, inner3);
    String s =
        formatter.format(statement, StatementFormatVerbosity.EXTENDED, version, codecRegistry);
    assertThat(s.replaceAll("\\s", ""))
        .contains("BatchStatement@")
        .contains("[UNLOGGED,3stmts,8values]")
        .contains(
            formatter
                .format(inner1, StatementFormatVerbosity.EXTENDED, version, codecRegistry)
                .replaceAll("\\s", ""))
        .contains(
            formatter
                .format(inner2, StatementFormatVerbosity.EXTENDED, version, codecRegistry)
                .replaceAll("\\s", ""))
        .doesNotContain(
            formatter
                .format(inner3, StatementFormatVerbosity.EXTENDED, version, codecRegistry)
                .replaceAll("\\s", ""))
        // test that max bound values is reset for each inner statement
        .contains("c1:'foo'")
        .contains("c2:<NULL>")
        .doesNotContain("c3:<UNSET>")
        .contains("0:'foo'")
        .contains("1:42")
        .doesNotContain("c3:'bar'")
        .contains("...");
  }

  @Test
  void should_override_default_printer_with_concrete_class() {
    SimpleStatement statement = SimpleStatement.newInstance("select * from system.local");
    String customAppend = "Used custom simple statement formatter";
    StatementFormatter formatter =
        StatementFormatter.builder()
            .addStatementPrinters(
                new StatementPrinter<SimpleStatement>() {

                  @Override
                  public Class<SimpleStatement> getSupportedStatementClass() {
                    return SimpleStatement.class;
                  }

                  @Override
                  public void print(
                      SimpleStatement statement,
                      StatementWriter out,
                      StatementFormatVerbosity verbosity) {
                    out.appendQueryStringFragment(statement.getQuery());
                    out.append(", ");
                    out.append(customAppend);
                  }
                })
            .build();

    String s =
        formatter.format(statement, StatementFormatVerbosity.EXTENDED, version, codecRegistry);
    assertThat(s).isEqualTo(statement.getQuery() + ", " + customAppend);
  }

  @Test
  void should_override_default_printer_with_ancestor_class() {
    SimpleStatement simpleStatement = SimpleStatement.newInstance("select * from system.local");
    String customAppend = "Used custom statement formatter";
    StatementFormatter formatter =
        StatementFormatter.builder()
            .addStatementPrinters(
                new StatementPrinter<BatchableStatement<?>>() {

                  @Override
                  public Class<?> getSupportedStatementClass() {
                    return BatchableStatement.class;
                  }

                  @Override
                  public void print(
                      BatchableStatement<?> statement,
                      StatementWriter out,
                      StatementFormatVerbosity verbosity) {
                    out.append(customAppend);
                  }
                })
            .build();
    // Should use custom formatter for BatchableStatement<?> implementations.
    String s =
        formatter.format(
            simpleStatement, StatementFormatVerbosity.EXTENDED, version, codecRegistry);
    assertThat(s).contains(customAppend);
  }

  // Data types

  @ParameterizedTest
  @ArgumentsSource(DataTypesProvider.class)
  void should_log_all_parameter_types_simple_statements(DataType type, Object value) {
    String query = "UPDATE test SET c1 = ? WHERE pk = 42";
    StatementFormatter formatter =
        StatementFormatter.builder()
            .withMaxBoundValueLength(StatementFormatterLimits.UNLIMITED)
            .build();
    SimpleStatement statement = SimpleStatement.newInstance(query, value);
    String s =
        formatter.format(statement, StatementFormatVerbosity.EXTENDED, version, codecRegistry);
    TypeCodec<Object> codec = codecRegistry.codecFor(type, value);
    assertThat(s).contains(codec.format(value));
  }

  @ParameterizedTest
  @ArgumentsSource(DataTypesProvider.class)
  void should_log_all_parameter_types_bound_statements(DataType type, Object value) {
    String query = "UPDATE test SET c1 = ? WHERE pk = 42";
    StatementFormatter formatter =
        StatementFormatter.builder()
            .withMaxBoundValueLength(StatementFormatterLimits.UNLIMITED)
            .build();
    BoundStatement statement = mockBoundStatement(query, value);
    String s =
        formatter.format(statement, StatementFormatVerbosity.EXTENDED, version, codecRegistry);
    assertThat(s).contains(codecRegistry.codecFor(type).format(value));
  }
}
