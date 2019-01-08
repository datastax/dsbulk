/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.dsbulk.engine.internal.schema.QueryInspector.INTERNAL_TIMESTAMP_VARNAME;
import static com.datastax.dsbulk.engine.internal.schema.QueryInspector.INTERNAL_TTL_VARNAME;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import com.google.common.collect.Lists;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class QueryInspectorTest {

  private static final CQLIdentifier NOW = CQLIdentifier.fromInternal("now");
  private static final CQLIdentifier MAX = CQLIdentifier.fromInternal("max");
  private static final CQLIdentifier SQRT = CQLIdentifier.fromInternal("sqrt");

  private static final FunctionCall FUNC_NOW = new FunctionCall(null, NOW);

  private static final CQLIdentifier KS = CQLIdentifier.fromInternal("ks");
  private static final CQLIdentifier MY_KS = CQLIdentifier.fromInternal("MyKs1");
  private static final CQLIdentifier MY_KEYSPACE = CQLIdentifier.fromInternal("MyKeyspace");

  private static final CQLIdentifier MY_TABLE = CQLIdentifier.fromInternal("MyTable");

  private static final CQLIdentifier COL_1 = CQLIdentifier.fromInternal("col1");
  private static final CQLIdentifier MY_COL_2 = CQLIdentifier.fromInternal("My Col 2");

  private static final CQLIdentifier FOO = CQLIdentifier.fromInternal("foo");
  private static final CQLIdentifier PK = CQLIdentifier.fromInternal("pk");
  private static final CQLIdentifier CC = CQLIdentifier.fromInternal("cc");
  private static final CQLIdentifier V = CQLIdentifier.fromInternal("v");
  private static final CQLIdentifier MY_PK = CQLIdentifier.fromInternal("My PK");
  private static final CQLIdentifier MY_CC = CQLIdentifier.fromInternal("My CC");
  private static final CQLIdentifier MY_VALUE = CQLIdentifier.fromInternal("My Value");
  private static final CQLIdentifier WRITETIME = CQLIdentifier.fromInternal("writetime");
  private static final CQLIdentifier TTL = CQLIdentifier.fromInternal("ttl");
  private static final CQLIdentifier MY_WRITETIME = CQLIdentifier.fromInternal("My Writetime");
  private static final CQLIdentifier MY_TTL = CQLIdentifier.fromInternal("My TTL");
  private static final CQLIdentifier T_1 = CQLIdentifier.fromInternal("t1");
  private static final CQLIdentifier T_2 = CQLIdentifier.fromInternal("t2");

  private static final CQLLiteral _16 = new CQLLiteral("16");
  private static final CQLLiteral _2 = new CQLLiteral("2");
  private static final CQLLiteral _3 = new CQLLiteral("3");

  @Test
  void should_detect_table_name_simple_insert() {
    QueryInspector inspector =
        new QueryInspector("INSERT INTO foo (pk, cc, v) VALUES (:pk, :cc, :v)");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(FOO);
  }

  @Test
  void should_detect_table_name_quoted_insert() {
    QueryInspector inspector =
        new QueryInspector("INSERT INTO \"MyTable\" (pk, cc, v) VALUES (:pk, :cc, :v)");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(MY_TABLE);
  }

  @Test
  void should_detect_keyspace_and_table_name_simple_insert() {
    QueryInspector inspector =
        new QueryInspector("INSERT INTO ks.foo (pk, cc, v) VALUES (:pk, :cc, :v)");
    assertThat(inspector.getKeyspaceName()).hasValue(KS);
    assertThat(inspector.getTableName()).isEqualTo(FOO);
  }

  @Test
  void should_detect_keyspace_and_table_name_quoted_insert() {
    QueryInspector inspector =
        new QueryInspector(
            "INSERT INTO \"MyKeyspace\".\"MyTable\" (pk, cc, v) VALUES (:pk, :cc, :v)");
    assertThat(inspector.getKeyspaceName()).hasValue(MY_KEYSPACE);
    assertThat(inspector.getTableName()).isEqualTo(MY_TABLE);
  }

  @Test
  void should_detect_table_name_simple_update() {
    QueryInspector inspector = new QueryInspector("UPDATE foo SET v = ? WHERE pk = ?");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(FOO);
  }

  @Test
  void should_detect_table_name_quoted_update() {
    QueryInspector inspector = new QueryInspector("UPDATE \"MyTable\" SET v = ? WHERE pk = ?");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(MY_TABLE);
  }

  @Test
  void should_detect_keyspace_and_table_name_simple_update() {
    QueryInspector inspector = new QueryInspector("UPDATE ks.foo SET v = ? WHERE pk = ?");
    assertThat(inspector.getKeyspaceName()).hasValue(KS);
    assertThat(inspector.getTableName()).isEqualTo(FOO);
  }

  @Test
  void should_detect_keyspace_and_table_name_quoted_update() {
    QueryInspector inspector =
        new QueryInspector("UPDATE \"MyKeyspace\".\"MyTable\" SET v = ? WHERE pk = ?");
    assertThat(inspector.getKeyspaceName()).hasValue(MY_KEYSPACE);
    assertThat(inspector.getTableName()).isEqualTo(MY_TABLE);
  }

  @Test
  void should_detect_table_name_simple_select() {
    QueryInspector inspector = new QueryInspector("SELECT c1 FROM foo");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(FOO);
  }

  @Test
  void should_detect_table_name_quoted_select() {
    QueryInspector inspector = new QueryInspector("SELECT c1 FROM \"MyTable\"");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(MY_TABLE);
  }

  @Test
  void should_detect_keyspace_and_table_name_simple_select() {
    QueryInspector inspector = new QueryInspector("SELECT c1 FROM ks.foo");
    assertThat(inspector.getKeyspaceName()).hasValue(KS);
    assertThat(inspector.getTableName()).isEqualTo(FOO);
  }

  @Test
  void should_detect_keyspace_and_table_name_quoted_select() {
    QueryInspector inspector = new QueryInspector("SELECT c1 FROM \"MyKeyspace\".\"MyTable\"");
    assertThat(inspector.getKeyspaceName()).hasValue(MY_KEYSPACE);
    assertThat(inspector.getTableName()).isEqualTo(MY_TABLE);
  }

  @Test
  void should_detect_table_name_simple_delete() {
    QueryInspector inspector = new QueryInspector("DELETE c1 FROM foo WHERE pk = 1");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(FOO);
  }

  @Test
  void should_detect_table_name_quoted_delete() {
    QueryInspector inspector = new QueryInspector("DELETE c1 FROM \"MyTable\" WHERE pk = 1");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(MY_TABLE);
  }

  @Test
  void should_detect_keyspace_and_table_name_simple_delete() {
    QueryInspector inspector = new QueryInspector("DELETE c1 FROM ks.foo WHERE pk = 1");
    assertThat(inspector.getKeyspaceName()).hasValue(KS);
    assertThat(inspector.getTableName()).isEqualTo(FOO);
  }

  @Test
  void should_detect_keyspace_and_table_name_quoted_delete() {
    QueryInspector inspector =
        new QueryInspector("DELETE c1 FROM \"MyKeyspace\".\"MyTable\" WHERE pk = 1");
    assertThat(inspector.getKeyspaceName()).hasValue(MY_KEYSPACE);
    assertThat(inspector.getTableName()).isEqualTo(MY_TABLE);
  }

  @Test
  void should_detect_named_variable_insert() {
    QueryInspector inspector =
        new QueryInspector("INSERT INTO ks.foo (pk, cc, v) VALUES (:pk, :cc, :v)");
    assertThat(inspector.getAssignments())
        .containsEntry(PK, PK)
        .containsEntry(CC, CC)
        .containsEntry(V, V);
  }

  @Test
  void should_detect_named_variable_quoted_insert() {
    QueryInspector inspector =
        new QueryInspector(
            "INSERT INTO ks.foo (\"My PK\", \"My CC\", \"My Value\") VALUES (:\"My PK\", :\"My CC\", :\"My Value\")");
    assertThat(inspector.getAssignments())
        .containsEntry(MY_PK, MY_PK)
        .containsEntry(MY_CC, MY_CC)
        .containsEntry(MY_VALUE, MY_VALUE);
  }

  @Test
  void should_detect_positional_variable_insert() {
    QueryInspector inspector = new QueryInspector("INSERT INTO ks.foo (pk, cc, v) VALUES (?,?,?)");
    assertThat(inspector.getAssignments())
        .containsEntry(PK, PK)
        .containsEntry(CC, CC)
        .containsEntry(V, V);
  }

  @Test
  void should_detect_function_variable_insert() {
    QueryInspector inspector =
        new QueryInspector("INSERT INTO ks.foo (pk, cc, v) VALUES (?,?,now())");
    assertThat(inspector.getAssignments())
        .containsEntry(PK, PK)
        .containsEntry(CC, CC)
        .containsEntry(V, FUNC_NOW);
  }

  @Test
  void should_detect_named_variable_update() {
    QueryInspector inspector =
        new QueryInspector("UPDATE ks.foo SET v = v + :v WHERE pk = :pk AND cc = :cc");
    assertThat(inspector.getAssignments())
        .containsEntry(PK, PK)
        .containsEntry(CC, CC)
        .containsEntry(V, V);
  }

  @Test
  void should_detect_named_variable_update_shorthand_notation() {
    QueryInspector inspector =
        new QueryInspector("UPDATE ks.foo SET v += :v WHERE pk = :pk AND cc = :cc");
    assertThat(inspector.getAssignments())
        .containsEntry(PK, PK)
        .containsEntry(CC, CC)
        .containsEntry(V, V);
  }

  @Test
  void should_detect_named_variable_quoted_update() {
    QueryInspector inspector =
        new QueryInspector(
            "UPDATE ks.foo SET \"My Value\" = \"My Value\" + :\"My Value\" "
                + "WHERE \"My PK\" = :\"My PK\" AND \"My CC\" = :\"My CC\"");
    assertThat(inspector.getAssignments())
        .containsEntry(MY_PK, MY_PK)
        .containsEntry(MY_CC, MY_CC)
        .containsEntry(MY_VALUE, MY_VALUE);
  }

  @Test
  void should_detect_positional_variable_update() {
    QueryInspector inspector =
        new QueryInspector("UPDATE ks.foo SET v = v + ? WHERE pk = ? AND cc = ?");
    assertThat(inspector.getAssignments())
        .containsEntry(PK, PK)
        .containsEntry(CC, CC)
        .containsEntry(V, V);
  }

  @Test
  void should_detect_function_variable_update() {
    QueryInspector inspector =
        new QueryInspector("UPDATE ks.foo SET v = v + now() WHERE pk = ? AND cc = ?");
    assertThat(inspector.getAssignments())
        .containsEntry(PK, PK)
        .containsEntry(CC, CC)
        .containsEntry(V, FUNC_NOW);
  }

  @Test
  void should_detect_named_variable_delete() {
    QueryInspector inspector = new QueryInspector("DELETE FROM ks.foo WHERE pk = :pk AND cc = :cc");
    assertThat(inspector.getAssignments()).containsEntry(PK, PK).containsEntry(CC, CC);
  }

  @Test
  void should_detect_named_variable_quoted_delete() {
    QueryInspector inspector =
        new QueryInspector(
            "DELETE FROM ks.foo WHERE \"My PK\" = :\"My PK\" AND \"My CC\" = :\"My CC\"");
    assertThat(inspector.getAssignments()).containsEntry(MY_PK, MY_PK).containsEntry(MY_CC, MY_CC);
  }

  @Test
  void should_detect_positional_variable_delete() {
    QueryInspector inspector = new QueryInspector("DELETE FROM ks.foo WHERE pk = ? AND cc = ?");
    assertThat(inspector.getAssignments()).containsEntry(PK, PK).containsEntry(CC, CC);
  }

  @Test
  void should_detect_function_variable_delete() {
    QueryInspector inspector = new QueryInspector("DELETE FROM ks.foo WHERE pk = ? AND cc = now()");
    assertThat(inspector.getAssignments()).containsEntry(PK, PK).containsEntry(CC, FUNC_NOW);
  }

  @Test
  void should_detect_named_variable_select() {
    QueryInspector inspector = new QueryInspector("SELECT pk, cc, v FROM ks.foo");
    assertThat(inspector.getResultSetVariables().keySet()).contains(PK, CC, V);
    assertThat(inspector.getResultSetVariables().values()).contains(PK, CC, V);
  }

  @Test
  void should_detect_named_variable_quoted_select() {
    QueryInspector inspector =
        new QueryInspector("SELECT \"My PK\", \"My CC\",\"My Value\" FROM ks.foo");
    assertThat(inspector.getResultSetVariables().keySet()).contains(MY_PK, MY_CC, MY_VALUE);
    assertThat(inspector.getResultSetVariables().values()).contains(MY_PK, MY_CC, MY_VALUE);
  }

  @Test
  void should_detect_named_variable_aliased_select() {
    QueryInspector inspector =
        new QueryInspector(
            "SELECT pk AS \"My PK\", cc AS \"My CC\", v AS \"My Value\" FROM ks.foo");
    assertThat(inspector.getResultSetVariables().keySet()).contains(PK, CC, V);
    assertThat(inspector.getResultSetVariables().values()).contains(MY_PK, MY_CC, MY_VALUE);
  }

  @Test
  void should_detect_writetime_insert() {
    QueryInspector inspector =
        new QueryInspector(
            "INSERT INTO ks.foo (pk, cc, v) VALUES (?, ?, ?) USING TTL :ttl AND TIMESTAMP :writetime");
    assertThat(inspector.getWriteTimeVariables()).hasSize(1).containsOnly(WRITETIME);
    assertThat(inspector.getUsingTimestampVariable()).contains(WRITETIME);
    assertThat(inspector.getUsingTTLVariable()).contains(TTL);
  }

  @Test
  void should_detect_writetime_quoted_insert() {
    QueryInspector inspector =
        new QueryInspector(
            "INSERT INTO ks.foo (pk, cc, v) VALUES (?, ?, ?) "
                + "USING TTL :\"My TTL\" AND TIMESTAMP :\"My Writetime\"");
    assertThat(inspector.getWriteTimeVariables()).hasSize(1).containsOnly(MY_WRITETIME);
    assertThat(inspector.getUsingTimestampVariable()).contains(MY_WRITETIME);
    assertThat(inspector.getUsingTTLVariable()).contains(MY_TTL);
  }

  @Test
  void should_detect_writetime_update() {
    QueryInspector inspector =
        new QueryInspector(
            "UPDATE ks.foo USING TTL :ttl AND TIMESTAMP :writetime SET foo = :bar WHERE pk = 1");
    assertThat(inspector.getWriteTimeVariables()).hasSize(1).containsOnly(WRITETIME);
    assertThat(inspector.getUsingTimestampVariable()).contains(WRITETIME);
    assertThat(inspector.getUsingTTLVariable()).contains(TTL);
  }

  @Test
  void should_detect_writetime_quoted_update() {
    QueryInspector inspector =
        new QueryInspector(
            "UPDATE ks.foo USING TTL :\"My TTL\" AND TiMeStAmP :\"My Writetime\" SET foo = :bar WHERE pk = 1");
    assertThat(inspector.getWriteTimeVariables()).hasSize(1).containsOnly(MY_WRITETIME);
    assertThat(inspector.getUsingTimestampVariable()).contains(MY_WRITETIME);
    assertThat(inspector.getUsingTTLVariable()).contains(MY_TTL);
  }

  @Test
  void should_detect_writetime_delete() {
    QueryInspector inspector =
        new QueryInspector("DELETE FROM ks.foo USING TIMESTAMP :writetime WHERE pk = 1");
    assertThat(inspector.getWriteTimeVariables()).hasSize(1).containsOnly(WRITETIME);
    assertThat(inspector.getUsingTimestampVariable()).contains(WRITETIME);
  }

  @Test
  void should_detect_writetime_quoted_delete() {
    QueryInspector inspector =
        new QueryInspector("DELETE FROM ks.foo USING TiMeStAmP :\"My Writetime\" WHERE pk = 1");
    assertThat(inspector.getWriteTimeVariables()).hasSize(1).containsOnly(MY_WRITETIME);
    assertThat(inspector.getUsingTimestampVariable()).contains(MY_WRITETIME);
  }

  @Test
  void should_detect_writetime_insert_positional() {
    QueryInspector inspector =
        new QueryInspector(
            "INSERT INTO ks.foo (pk, cc, v) VALUES (?, ?, ?) USING TTL ? AND TIMESTAMP ?");
    assertThat(inspector.getWriteTimeVariables())
        .hasSize(1)
        .containsOnly(INTERNAL_TIMESTAMP_VARNAME);
    assertThat(inspector.getUsingTimestampVariable()).contains(INTERNAL_TIMESTAMP_VARNAME);
    assertThat(inspector.getUsingTTLVariable()).contains(INTERNAL_TTL_VARNAME);
  }

  @Test
  void should_detect_writetime_update_positional() {
    QueryInspector inspector =
        new QueryInspector("UPDATE ks.foo USING TTL ? AND TIMESTAMP ? SET foo = ? WHERE pk = 1");
    assertThat(inspector.getWriteTimeVariables())
        .hasSize(1)
        .containsOnly(INTERNAL_TIMESTAMP_VARNAME);
    assertThat(inspector.getUsingTimestampVariable()).contains(INTERNAL_TIMESTAMP_VARNAME);
    assertThat(inspector.getUsingTTLVariable()).contains(INTERNAL_TTL_VARNAME);
  }

  @Test
  void should_detect_writetime_delete_positional() {
    QueryInspector inspector =
        new QueryInspector("DELETE FROM ks.foo USING TIMESTAMP ? WHERE pk = 1");
    assertThat(inspector.getWriteTimeVariables())
        .hasSize(1)
        .containsOnly(INTERNAL_TIMESTAMP_VARNAME);
    assertThat(inspector.getUsingTimestampVariable()).contains(INTERNAL_TIMESTAMP_VARNAME);
  }

  @Test
  void should_detect_writetime_select() {
    QueryInspector inspector = new QueryInspector("SELECT WrItEtImE(mycol) FROM ks.foo");
    assertThat(inspector.getWriteTimeVariables())
        .hasSize(1)
        .containsOnly(new FunctionCall(null, WRITETIME, CQLIdentifier.fromInternal("mycol")));
  }

  @Test
  void should_detect_writetime_quoted_select() {
    QueryInspector inspector = new QueryInspector("SELECT WrItEtImE(\"My Col\") FROM ks.foo");
    assertThat(inspector.getWriteTimeVariables())
        .hasSize(1)
        .containsOnly(new FunctionCall(null, WRITETIME, CQLIdentifier.fromInternal("My Col")));
  }

  @Test
  void should_detect_writetime_select_aliased() {
    QueryInspector inspector =
        new QueryInspector("SELECT WrItEtImE(mycol) AS WRITETIME FROM ks.foo");
    assertThat(inspector.getWriteTimeVariables()).hasSize(1).containsOnly(WRITETIME);
  }

  @Test
  void should_detect_writetime_quoted_select_aliased() {
    QueryInspector inspector =
        new QueryInspector("SELECT WrItEtImE(\"My Col\") AS \"My Writetime\" FROM ks.foo");
    assertThat(inspector.getWriteTimeVariables()).hasSize(1).containsOnly(MY_WRITETIME);
  }

  @Test
  void should_detect_multiple_writetime_select() {
    QueryInspector inspector =
        new QueryInspector("SELECT writetime(col1) as t1, writetime(col2) as t2 FROM ks.foo");
    assertThat(inspector.getWriteTimeVariables()).hasSize(2).containsExactly(T_1, T_2);
  }

  @Test
  void should_detect_ttl_select() {
    QueryInspector inspector =
        new QueryInspector("SELECT TTL(col1) as t1, ttl(\"My Col 2\") FROM ks.foo");
    FunctionCall ttl1 = new FunctionCall(null, TTL, COL_1);
    FunctionCall ttl2 = new FunctionCall(null, TTL, MY_COL_2);
    assertThat(inspector.getResultSetVariables())
        .hasSize(2)
        .containsKeys(ttl1, ttl2)
        .containsValues(T_1, ttl2);
  }

  @Test
  void should_detect_function_select() {
    QueryInspector inspector =
        new QueryInspector(
            "SELECT myFunction(col1) as f1, \"MyFunction\"(\"My Col 2\") FROM ks.foo");
    CQLIdentifier name1 = CQLIdentifier.fromInternal("myfunction");
    CQLIdentifier name2 = CQLIdentifier.fromInternal("MyFunction");
    FunctionCall f1 = new FunctionCall(null, name1, COL_1);
    FunctionCall f2 = new FunctionCall(null, name2, MY_COL_2);
    assertThat(inspector.getResultSetVariables())
        .hasSize(2)
        .containsKeys(f1, f2)
        .containsValues(CQLIdentifier.fromInternal("f1"), f2);
  }

  @Test
  void should_error_out_if_syntax_error() {
    assertThatThrownBy(() -> new QueryInspector(" not a valid statement "))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid query: ' not a valid statement ' could not be parsed at line");
  }

  @Test
  void should_error_out_if_insert_json() {
    assertThatThrownBy(() -> new QueryInspector("INSERT INTO table1 JSON '{ \"col1\" : value }'"))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("Invalid query: INSERT JSON is not supported:");
  }

  @Test
  void should_error_out_if_select_json() {
    assertThatThrownBy(() -> new QueryInspector("SELECT JSON col1 FROM table1"))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("Invalid query: SELECT JSON is not supported:");
  }

  @Test
  void should_error_out_if_insert_values_mismatch() {
    assertThatThrownBy(() -> new QueryInspector("INSERT INTO ks.foo (pk, cc, v) VALUES (?, ?)"))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid query: the number of columns to insert (3) does not match the number of terms (2)");
    assertThatThrownBy(() -> new QueryInspector("INSERT INTO ks.foo (pk, cc, v) VALUES (:pk, :cc)"))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid query: the number of columns to insert (3) does not match the number of terms (2)");
  }

  @Test
  void should_detect_from_clause_start_index() {
    QueryInspector inspector = new QueryInspector("SELECT col1  \n\t   FROM ks.foo");
    assertThat(inspector.getFromClauseStartIndex()).isEqualTo("SELECT col1  \n\t   ".length());
  }

  @Test
  void should_detect_from_clause_end_index_when_where_clause_absent() {
    String query = "SELECT a,b,c FROM ks.t1";
    QueryInspector inspector = new QueryInspector(query);
    assertThat(inspector.hasWhereClause()).isFalse();
    assertThat(inspector.getFromClauseEndIndex()).isEqualTo(query.length() - 1);
  }

  @Test
  void should_detect_from_clause_end_index_when_where_clause_absent_but_limit_clause_present() {
    String query = "SELECT a,b,c FROM ks.t1 LIMIT 100";
    QueryInspector inspector = new QueryInspector(query);
    assertThat(inspector.hasWhereClause()).isFalse();
    assertThat(inspector.getFromClauseEndIndex()).isEqualTo("SELECT a,b,c FROM ks.t1".length() - 1);
  }

  @Test
  void should_detect_from_clause_end_index_when_where_clause_present() {
    String query = "SELECT a,b,c FROM ks.t1 WHERE pk = 0 LIMIT 100";
    QueryInspector inspector = new QueryInspector(query);
    assertThat(inspector.hasWhereClause()).isTrue();
    assertThat(inspector.getFromClauseEndIndex()).isEqualTo("SELECT a,b,c FROM ks.t1".length() - 1);
  }

  @ParameterizedTest
  @MethodSource
  void should_report_named_token_range_restriction_variables(
      String query, int startIndex, int endIndex) {
    QueryInspector inspector = new QueryInspector(query);
    assertThat(inspector.getTokenRangeRestrictionStartVariable())
        .contains(CQLIdentifier.fromInternal("begin"));
    assertThat(inspector.getTokenRangeRestrictionEndVariable())
        .contains(CQLIdentifier.fromInternal("finish"));
    assertThat(inspector.getTokenRangeRestrictionStartVariableIndex()).isEqualTo(startIndex);
    assertThat(inspector.getTokenRangeRestrictionEndVariableIndex()).isEqualTo(endIndex);
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_report_named_token_range_restriction_variables() {
    return Lists.newArrayList(
        arguments(
            "SELECT a,b,c FROM ks.t1 WHERE token(pk) > :\"begin\" AND token(pk) <= :finish", 0, 1),
        arguments(
            "SELECT a,b,c FROM ks.t1 WHERE token(pk) <= :finish AND token(pk) > :\"begin\"", 1, 0),
        arguments(
            "SELECT a,b,c FROM ks.t1 WHERE foo = 42 AND token(pk) <= :finish AND token(pk) > :\"begin\"",
            1,
            0));
  }

  @ParameterizedTest
  @MethodSource
  void should_report_positional_token_range_restriction_variables(
      String query, int startIndex, int endIndex) {
    QueryInspector inspector = new QueryInspector(query);
    assertThat(inspector.getTokenRangeRestrictionStartVariable())
        .contains(CQLIdentifier.fromInternal("partition key token"));
    assertThat(inspector.getTokenRangeRestrictionEndVariable())
        .contains(CQLIdentifier.fromInternal("partition key token"));
    assertThat(inspector.getTokenRangeRestrictionStartVariableIndex()).isEqualTo(startIndex);
    assertThat(inspector.getTokenRangeRestrictionEndVariableIndex()).isEqualTo(endIndex);
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_report_positional_token_range_restriction_variables() {
    return Lists.newArrayList(
        arguments("SELECT a,b,c FROM ks.t1 WHERE token(pk) > ? AND token(pk) <= ?", 0, 1),
        arguments("SELECT a,b,c FROM ks.t1 WHERE token(pk) <= ? AND token(pk) > ?", 1, 0),
        arguments(
            "SELECT a,b,c FROM ks.t1 WHERE foo = 42 AND token(pk) <= ? AND token(pk) > ?", 1, 0));
  }

  @ParameterizedTest
  @MethodSource
  void should_detect_select_star(String query, boolean expected) {
    assertThat(new QueryInspector(query).isSelectStar()).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_detect_select_star() {
    return Lists.newArrayList(
        arguments("SELECT * FROM ks.t1", true),
        arguments("SELECT a FROM ks.t1", false),
        arguments("SELECT a,b,c FROM ks.t1", false));
  }

  @ParameterizedTest
  @MethodSource
  void should_detect_unsupported_selector(String query, boolean expected) {
    assertThat(new QueryInspector(query).hasUnsupportedSelectors()).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_detect_unsupported_selector() {
    return Lists.newArrayList(
        arguments("SELECT myUdt.myField FROM ks.t1", true),
        arguments("SELECT COUNT(*) FROM ks.t1", true),
        arguments("SELECT CAST (c1 as int) FROM ks.t1", true),
        arguments("SELECT (int) 1 FROM ks.t1", true),
        arguments("SELECT c1, writetime(c1), ttl(c1), now() FROM ks.t1", false),
        arguments(
            "SELECT c1, writetime(c1) AS wrt, ttl(c1) as ttl, now() as now FROM ks.t1", false),
        arguments("SELECT * FROM ks.t1", false));
  }

  @ParameterizedTest
  @MethodSource
  void should_detect_functions_in_assignments(
      String query, int expectedTotalAssignments, FunctionCall expectedValue) {
    QueryInspector inspector = new QueryInspector(query);
    assertThat(inspector.getAssignments())
        .hasSize(expectedTotalAssignments)
        .containsValue(expectedValue);
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_detect_functions_in_assignments() {
    return Lists.newArrayList(
        // INSERT
        arguments("INSERT INTO table1 (pk, v) VALUES (0, now())", 2, FUNC_NOW),
        arguments(
            "INSERT INTO table1 (pk, v) VALUES (0, sqrt(16))",
            2,
            new FunctionCall(null, SQRT, _16)),
        arguments(
            "INSERT INTO table1 (pk, v) VALUES (0, max(2, 3))",
            2,
            new FunctionCall(null, MAX, _2, _3)),
        // INSERT, qualified
        arguments("INSERT INTO table1 (pk, v) VALUES (0, ks.now())", 2, new FunctionCall(KS, NOW)),
        arguments(
            "INSERT INTO table1 (pk, v) VALUES (0, \"MyKs1\".sqrt(16))",
            2,
            new FunctionCall(MY_KS, SQRT, _16)),
        arguments(
            "INSERT INTO table1 (pk, v) VALUES (0, ks.max(2, 3))",
            2,
            new FunctionCall(KS, MAX, _2, _3)),
        // UPDATE
        arguments("UPDATE table1 set v = now() WHERE pk = 0", 2, FUNC_NOW),
        arguments(
            "UPDATE table1 set v = sqrt(16) WHERE pk = 0", 2, new FunctionCall(null, SQRT, _16)),
        arguments(
            "UPDATE table1 set v = max(2, 3) WHERE pk = 0", 2, new FunctionCall(null, MAX, _2, _3)),
        // UPDATE, qualified
        arguments("UPDATE table1 set v = ks.now() WHERE pk = 0", 2, new FunctionCall(KS, NOW)),
        arguments(
            "UPDATE table1 set v = \"MyKs1\".sqrt(16) WHERE pk = 0",
            2,
            new FunctionCall(MY_KS, SQRT, _16)),
        arguments(
            "UPDATE table1 set v = ks.max(2, 3) WHERE pk = 0",
            2,
            new FunctionCall(KS, MAX, _2, _3)));
  }

  @ParameterizedTest
  @MethodSource
  void should_detect_functions_in_result_set(String query, FunctionCall expectedValue) {
    QueryInspector inspector = new QueryInspector(query);
    assertThat(inspector.getResultSetVariables()).hasSize(1).containsValue(expectedValue);
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_detect_functions_in_result_set() {
    return Lists.newArrayList(
        // SELECT
        arguments("SELECT now() FROM table1", FUNC_NOW),
        arguments(
            "SELECT sqrt(c1) FROM table1",
            new FunctionCall(null, SQRT, CQLIdentifier.fromInternal("c1"))),
        arguments(
            "SELECT max(c1, c2)  FROM table1",
            new FunctionCall(
                null, MAX, CQLIdentifier.fromInternal("c1"), CQLIdentifier.fromInternal("c2"))),
        // SELECT, qualified
        arguments("SELECT ks.now() FROM table1", new FunctionCall(KS, NOW)),
        arguments(
            "SELECT \"MyKs1\".sqrt(c1) FROM table1",
            new FunctionCall(MY_KS, SQRT, CQLIdentifier.fromInternal("c1"))),
        arguments(
            "SELECT ks.max(c1, c2)  FROM table1",
            new FunctionCall(
                KS, MAX, CQLIdentifier.fromInternal("c1"), CQLIdentifier.fromInternal("c2"))));
  }
}
