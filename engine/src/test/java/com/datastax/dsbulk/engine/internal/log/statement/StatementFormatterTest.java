/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.log.statement;

import static com.datastax.driver.core.DriverCoreEngineTestHooks.newPreparedId;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatVerbosity.ABRIDGED;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatVerbosity.EXTENDED;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatVerbosity.NORMAL;
import static com.datastax.dsbulk.engine.internal.log.statement.StatementFormatterLimits.UNLIMITED;
import static com.google.common.base.Strings.repeat;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.ColumnDefinitions;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.DataType;
import com.datastax.driver.core.Duration;
import com.datastax.driver.core.LocalDate;
import com.datastax.driver.core.PreparedId;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.SimpleStatement;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.StatementWrapper;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.querybuilder.BuiltStatement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.schemabuilder.SchemaBuilder;
import com.datastax.driver.core.schemabuilder.SchemaStatement;
import com.datastax.driver.core.utils.Bytes;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.UUID;
import org.junit.Test;

public class StatementFormatterTest {

  private static final List<DataType> dataTypes =
      new ArrayList<>(
          Sets.filter(DataType.allPrimitiveTypes(), type -> type != DataType.counter()));

  private final ProtocolVersion version = ProtocolVersion.NEWEST_SUPPORTED;

  private final CodecRegistry codecRegistry = CodecRegistry.DEFAULT_INSTANCE;

  // Basic Tests

  @Test
  public void should_format_simple_statement() throws Exception {
    StatementFormatter formatter = StatementFormatter.builder().build();
    Statement statement =
        new SimpleStatement("SELECT * FROM t WHERE c1 = ? AND c2 = ?", "foo", 42)
            .setIdempotent(true)
            .setConsistencyLevel(ConsistencyLevel.QUORUM)
            .setSerialConsistencyLevel(ConsistencyLevel.SERIAL)
            .setDefaultTimestamp(12345L)
            .setReadTimeoutMillis(12345);
    String s = formatter.format(statement, EXTENDED, version, codecRegistry);
    assertThat(s)
        .contains("SimpleStatement@")
        .contains(
            "idempotence : true, CL : QUORUM, serial CL : SERIAL, default timestamp : 12345, read-timeout millis : 12345")
        .contains("2 values")
        .contains("SELECT * FROM t WHERE c1 = ? AND c2 = ?")
        .contains("0 : 'foo'")
        .contains("1 : 42");
  }

  @Test
  public void should_format_simple_statement_with_named_values() throws Exception {
    StatementFormatter formatter = StatementFormatter.builder().build();
    Statement statement =
        new SimpleStatement(
            "SELECT * FROM t WHERE c1 = ? AND c2 = ?", ImmutableMap.of("c1", "foo", "c2", 42));
    String s = formatter.format(statement, EXTENDED, version, codecRegistry);
    assertThat(s)
        .contains("SimpleStatement@")
        .contains("2 values")
        .contains("SELECT * FROM t WHERE c1 = ? AND c2 = ?")
        .contains("c1 : 'foo'")
        .contains("c2 : 42");
  }

  @Test
  public void should_format_statement_without_values() throws Exception {
    StatementFormatter formatter = StatementFormatter.builder().build();
    Statement statement = new SimpleStatement("SELECT * FROM t WHERE c1 = 42");
    String s = formatter.format(statement, EXTENDED, version, codecRegistry);
    assertThat(s)
        .contains("SimpleStatement@")
        .contains("0 values")
        .contains("SELECT * FROM t WHERE c1 = 42")
        .doesNotContain("{");
  }

  @Test
  public void should_format_built_statement() throws Exception {
    StatementFormatter formatter = StatementFormatter.builder().build();
    Statement statement =
        select().from("t").where(eq("c1", "foo")).and(eq("c2", 42)).and(eq("c3", false));
    String s = formatter.format(statement, EXTENDED, version, codecRegistry);
    assertThat(s)
        .contains("BuiltStatement@")
        .contains("2 values")
        .contains("SELECT * FROM t WHERE c1=? AND c2=42 AND c3=?")
        .contains("0 : 'foo'")
        .contains("1 : false");
  }

  @Test
  public void should_format_bound_statement() throws Exception {
    StatementFormatter formatter = StatementFormatter.builder().build();
    BoundStatement statement = newBoundStatementMock();
    String s = formatter.format(statement, EXTENDED, version, codecRegistry);
    assertThat(s)
        .contains("BoundStatement@")
        .contains("3 values")
        .contains("SELECT * FROM t WHERE c1 = ? AND c2 = ? AND c3 = ?")
        .contains("c1 : 'foo'")
        .contains("c2 : <NULL>")
        .contains("c3 : <UNSET>");
  }

  @Test
  public void should_format_batch_statement() throws Exception {
    StatementFormatter formatter = StatementFormatter.builder().build();
    BatchStatement statement = new BatchStatement(BatchStatement.Type.UNLOGGED);
    Statement inner1 = newBoundStatementMock();
    Statement inner2 = new SimpleStatement("SELECT * FROM t WHERE c1 = ? AND c2 = ?", "foo", 42);
    Statement inner3 = select().from("t").where(eq("c1", "foo")).and(eq("c2", 42));
    statement.add(inner1);
    statement.add(inner2);
    statement.add(inner3);
    String s = formatter.format(statement, EXTENDED, version, codecRegistry);
    assertThat(s.replaceAll("\\s", ""))
        .contains("BatchStatement@")
        .contains("[UNLOGGED,3stmts,6values]")
        .contains(
            "1:" + formatter.format(inner1, EXTENDED, version, codecRegistry).replaceAll("\\s", ""))
        .contains(
            "2:" + formatter.format(inner2, EXTENDED, version, codecRegistry).replaceAll("\\s", ""))
        .contains(
            "3:"
                + formatter.format(inner3, EXTENDED, version, codecRegistry).replaceAll("\\s", ""));
  }

  @Test
  public void should_format_wrapped_statement() throws Exception {
    StatementFormatter formatter = StatementFormatter.builder().build();
    Statement statement =
        new StatementWrapper(new SimpleStatement("SELECT * FROM t WHERE c1 = 42")) {};
    String s = formatter.format(statement, EXTENDED, version, codecRegistry);
    assertThat(s)
        .contains("SimpleStatement@")
        .contains("0 values")
        .contains("SELECT * FROM t WHERE c1 = 42")
        .doesNotContain("{");
  }

  // Verbosity

  @Test
  public void should_format_with_abridged_verbosity() throws Exception {
    StatementFormatter formatter = StatementFormatter.builder().build();
    Statement statement = new SimpleStatement("SELECT * FROM t WHERE c1 = ? AND c2 = ?", "foo", 42);
    String s = formatter.format(statement, ABRIDGED, version, codecRegistry);
    assertThat(s)
        .contains("SimpleStatement@")
        .doesNotContain("IDP")
        .doesNotContain("CL")
        .doesNotContain("SCL")
        .doesNotContain("RTM")
        .doesNotContain("DTP")
        .contains("2 values")
        .doesNotContain("SELECT * FROM t WHERE c1 = ? AND c2 = ?")
        .doesNotContain("{ 0 : 'foo', 1 : 42 }");
  }

  @Test
  public void should_format_with_normal_verbosity() throws Exception {
    StatementFormatter formatter = StatementFormatter.builder().build();
    Statement statement = new SimpleStatement("SELECT * FROM t WHERE c1 = ? AND c2 = ?", "foo", 42);
    String s = formatter.format(statement, NORMAL, version, codecRegistry);
    assertThat(s)
        .contains("SimpleStatement@")
        .doesNotContain("IDP")
        .doesNotContain("CL")
        .doesNotContain("SCL")
        .doesNotContain("SCL")
        .doesNotContain("RTM")
        .doesNotContain("DTP")
        .contains("2 values")
        .contains("SELECT * FROM t WHERE c1 = ? AND c2 = ?")
        .doesNotContain("{ 0 : 'foo', 1 : 42 }");
  }

  // Limits

  @Test
  public void should_truncate_query_string() throws Exception {
    StatementFormatter formatter = StatementFormatter.builder().withMaxQueryStringLength(7).build();
    SimpleStatement statement = new SimpleStatement("123456789");
    String s = formatter.format(statement, EXTENDED, version, codecRegistry);
    assertThat(s).contains("1234567...");
  }

  @Test
  public void should_not_truncate_query_string_when_unlimited() throws Exception {
    StatementFormatter formatter =
        StatementFormatter.builder().withMaxQueryStringLength(UNLIMITED).build();
    String query = repeat("a", 5000);
    SimpleStatement statement = new SimpleStatement(query);
    String s = formatter.format(statement, EXTENDED, version, codecRegistry);
    assertThat(s).contains(query);
  }

  @Test
  public void should_not_print_more_bound_values_than_max() throws Exception {
    StatementFormatter formatter = StatementFormatter.builder().withMaxBoundValues(2).build();
    SimpleStatement statement = new SimpleStatement("query", 0, 1, 2, 3);
    String s = formatter.format(statement, EXTENDED, version, codecRegistry);
    assertThat(s).contains("0 : 0").contains("1 : 1").contains("...");
  }

  @Test
  public void should_truncate_bound_value() throws Exception {
    StatementFormatter formatter = StatementFormatter.builder().withMaxBoundValueLength(4).build();
    SimpleStatement statement = new SimpleStatement("query", "12345");
    String s = formatter.format(statement, EXTENDED, version, codecRegistry);
    assertThat(s).contains("0 : '123...");
  }

  @Test
  public void should_truncate_bound_value_byte_buffer() throws Exception {
    StatementFormatter formatter = StatementFormatter.builder().withMaxBoundValueLength(4).build();
    SimpleStatement statement = new SimpleStatement("query", Bytes.fromHexString("0xCAFEBABE"));
    String s = formatter.format(statement, EXTENDED, version, codecRegistry);
    assertThat(s).contains("0 : 0xca...");
  }

  @Test
  public void should_truncate_inner_statements() throws Exception {
    StatementFormatter formatter =
        StatementFormatter.builder().withMaxInnerStatements(2).withMaxBoundValues(2).build();
    BatchStatement statement = new BatchStatement(BatchStatement.Type.UNLOGGED);
    Statement inner1 = newBoundStatementMock();
    Statement inner2 = new SimpleStatement("SELECT * FROM t WHERE c1 = ? AND c2 = ?", "foo", 42);
    Statement inner3 = select().from("t").where(eq("c1", "foo")).and(eq("c2", 42));
    statement.add(inner1);
    statement.add(inner2);
    statement.add(inner3);
    String s = formatter.format(statement, EXTENDED, version, codecRegistry);
    assertThat(s.replaceAll("\\s", ""))
        .contains("BatchStatement@")
        .contains("[UNLOGGED,3stmts,6values]")
        .contains(formatter.format(inner1, EXTENDED, version, codecRegistry).replaceAll("\\s", ""))
        .contains(formatter.format(inner2, EXTENDED, version, codecRegistry).replaceAll("\\s", ""))
        .doesNotContain(
            formatter.format(inner3, EXTENDED, version, codecRegistry).replaceAll("\\s", ""))
        // test that max bound values is reset for each inner statement
        .contains("c1:'foo'")
        .contains("c2:<NULL>")
        .contains("0:'foo'")
        .contains("1:42")
        .doesNotContain("c2:42")
        .contains("...");
  }

  @Test
  public void should_override_default_printer_with_concrete_class() throws Exception {
    SimpleStatement statement = new SimpleStatement("select * from system.local");
    final String customAppend = "Used custom simple statement formatter";
    StatementFormatter formatter =
        StatementFormatter.builder()
            .addStatementPrinter(
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
                    out.appendQueryStringFragment(statement.getQueryString());
                    out.append(", ");
                    out.append(customAppend);
                  }
                })
            .build();

    String s = formatter.format(statement, EXTENDED, version, codecRegistry);
    assertThat(s).isEqualTo(statement.getQueryString() + ", " + customAppend);
  }

  @Test
  public void should_override_default_printer_with_ancestor_class() throws Exception {
    SimpleStatement simpleStatement = new SimpleStatement("select * from system.local");
    BuiltStatement builtStatement = QueryBuilder.select().from("system", "peers");
    SchemaStatement schemaStatement =
        SchemaBuilder.createTable("x", "y").addPartitionKey("z", DataType.cint());
    BatchStatement batchStatement = new BatchStatement();
    final String customAppend = "Used custom statement formatter";
    StatementFormatter formatter =
        StatementFormatter.builder()
            .addStatementPrinter(
                new StatementPrinter<RegularStatement>() {

                  @Override
                  public Class<RegularStatement> getSupportedStatementClass() {
                    return RegularStatement.class;
                  }

                  @Override
                  public void print(
                      RegularStatement statement,
                      StatementWriter out,
                      StatementFormatVerbosity verbosity) {
                    out.appendQueryStringFragment(statement.getQueryString());
                    out.append(", ");
                    out.append(customAppend);
                  }
                })
            .build();

    // BatchStatement is not a regular statement so it should not be impacted.
    String s = formatter.format(batchStatement, EXTENDED, version, codecRegistry);
    assertThat(s).doesNotContain(customAppend);

    for (RegularStatement statement :
        Lists.newArrayList(simpleStatement, builtStatement, schemaStatement)) {
      // Should use custom formatter for RegularStatement implementations.
      s = formatter.format(statement, EXTENDED, version, codecRegistry);
      assertThat(s).isEqualTo(statement.getQueryString() + ", " + customAppend);
    }
  }

  // Data types

  @Test
  public void should_log_all_parameter_types_simple_statements() throws Exception {
    String query = "UPDATE test SET c1 = ? WHERE pk = 42";
    StatementFormatter formatter =
        StatementFormatter.builder().withMaxBoundValueLength(UNLIMITED).build();
    for (DataType type : dataTypes) {
      Object value = getFixedValue(type);
      SimpleStatement statement = new SimpleStatement(query, value);
      String s = formatter.format(statement, EXTENDED, version, codecRegistry);
      // time cannot be used with simple statements
      TypeCodec<Object> codec =
          codecRegistry.codecFor(type.equals(DataType.time()) ? DataType.bigint() : type, value);
      assertThat(s).contains(codec.format(value));
    }
  }

  @Test
  public void should_log_all_parameter_types_bound_statements() throws Exception {
    String query = "UPDATE test SET c1 = ? WHERE pk = 42";
    StatementFormatter formatter =
        StatementFormatter.builder().withMaxBoundValueLength(UNLIMITED).build();
    for (DataType type : dataTypes) {
      Object value = getFixedValue(type);
      BoundStatement statement = newBoundStatementMock(query, type);
      TypeCodec<Object> codec = codecRegistry.codecFor(type, value);
      statement.set(0, value, codec);
      String s = formatter.format(statement, EXTENDED, version, codecRegistry);
      assertThat(s).contains(codec.format(value));
    }
  }

  private BoundStatement newBoundStatementMock() {
    PreparedStatement ps = mock(PreparedStatement.class);
    ColumnDefinitions cd = mock(ColumnDefinitions.class);
    PreparedId pid = newPreparedId(cd, version);
    when(ps.getVariables()).thenReturn(cd);
    when(ps.getPreparedId()).thenReturn(pid);
    when(ps.getCodecRegistry()).thenReturn(codecRegistry);
    when(ps.getQueryString()).thenReturn("SELECT * FROM t WHERE c1 = ? AND c2 = ? AND c3 = ?");
    when(cd.size()).thenReturn(3);
    when(cd.getName(0)).thenReturn("c1");
    when(cd.getName(1)).thenReturn("c2");
    when(cd.getName(2)).thenReturn("c3");
    when(cd.getType(0)).thenReturn(DataType.varchar());
    when(cd.getType(1)).thenReturn(DataType.cint());
    when(cd.getType(2)).thenReturn(DataType.cboolean());
    BoundStatement statement = new BoundStatement(ps);
    statement.setString(0, "foo");
    statement.setToNull(1);
    return statement;
  }

  private BoundStatement newBoundStatementMock(String queryString, DataType type) {
    PreparedStatement ps = mock(PreparedStatement.class);
    ColumnDefinitions cd = mock(ColumnDefinitions.class);
    PreparedId pid = newPreparedId(cd, version);
    when(ps.getVariables()).thenReturn(cd);
    when(ps.getPreparedId()).thenReturn(pid);
    when(ps.getCodecRegistry()).thenReturn(codecRegistry);
    when(ps.getQueryString()).thenReturn(queryString);
    when(cd.size()).thenReturn(1);
    when(cd.getName(0)).thenReturn("c1");
    when(cd.getType(0)).thenReturn(type);
    return new BoundStatement(ps);
  }

  private static Object getFixedValue(final DataType type) {
    try {
      switch (type.getName()) {
        case ASCII:
          return "An ascii string";
        case BIGINT:
          return 42L;
        case BLOB:
          return ByteBuffer.wrap(new byte[] {(byte) 4, (byte) 12, (byte) 1});
        case BOOLEAN:
          return true;
        case COUNTER:
          throw new UnsupportedOperationException("Cannot 'getSomeValue' for counters");
        case DURATION:
          return Duration.from("1h20m3s");
        case DECIMAL:
          return new BigDecimal(
              "3.1415926535897932384626433832795028841971693993751058209749445923078164062862089986280348253421170679");
        case DOUBLE:
          return 3.142519;
        case FLOAT:
          return 3.142519f;
        case INET:
          return InetAddress.getByAddress(new byte[] {(byte) 127, (byte) 0, (byte) 0, (byte) 1});
        case TINYINT:
          return (byte) 25;
        case SMALLINT:
          return (short) 26;
        case INT:
          return 24;
        case TEXT:
          return "A text string";
        case TIMESTAMP:
          return new Date(1352288289L);
        case DATE:
          return LocalDate.fromDaysSinceEpoch(0);
        case TIME:
          return 54012123450000L;
        case UUID:
          return UUID.fromString("087E9967-CCDC-4A9B-9036-05930140A41B");
        case VARCHAR:
          return "A varchar string";
        case VARINT:
          return new BigInteger("123456789012345678901234567890");
        case TIMEUUID:
          return UUID.fromString("FE2B4360-28C6-11E2-81C1-0800200C9A66");
        case LIST:
          return new ArrayList<Object>() {
            {
              add(getFixedValue(type.getTypeArguments().get(0)));
            }
          };
        case SET:
          return new HashSet<Object>() {
            {
              add(getFixedValue(type.getTypeArguments().get(0)));
            }
          };
        case MAP:
          return new HashMap<Object, Object>() {
            {
              put(
                  getFixedValue(type.getTypeArguments().get(0)),
                  getFixedValue(type.getTypeArguments().get(1)));
            }
          };
      }
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    throw new RuntimeException("Missing handling of " + type);
  }
}
