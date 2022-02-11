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
package com.datastax.oss.dsbulk.workflow.commons.schema;

import static com.datastax.oss.dsbulk.workflow.commons.schema.QueryInspector.INTERNAL_TIMESTAMP_VARNAME;
import static com.datastax.oss.dsbulk.workflow.commons.schema.QueryInspector.INTERNAL_TTL_VARNAME;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import com.datastax.oss.dsbulk.mapping.CQLLiteral;
import com.datastax.oss.dsbulk.mapping.CQLWord;
import com.datastax.oss.dsbulk.mapping.FunctionCall;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class QueryInspectorTest {

  private static final CQLWord NOW = CQLWord.fromInternal("now");
  private static final CQLWord MAX = CQLWord.fromInternal("max");
  private static final CQLWord SQRT = CQLWord.fromInternal("sqrt");

  private static final FunctionCall FUNC_NOW = new FunctionCall(null, NOW);

  private static final CQLWord KS = CQLWord.fromInternal("ks");
  private static final CQLWord MY_KEYSPACE = CQLWord.fromInternal("MyKeyspace");

  private static final CQLWord TABLE_1 = CQLWord.fromInternal("table1");
  private static final CQLWord MY_TABLE = CQLWord.fromInternal("MyTable");

  private static final CQLWord COL_1 = CQLWord.fromInternal("col1");
  private static final CQLWord MY_COL_2 = CQLWord.fromInternal("My Col 2");

  private static final CQLWord PK = CQLWord.fromInternal("pk");
  private static final CQLWord CC = CQLWord.fromInternal("cc");
  private static final CQLWord V = CQLWord.fromInternal("v");
  private static final CQLWord MY_PK = CQLWord.fromInternal("My PK");
  private static final CQLWord MY_CC = CQLWord.fromInternal("My CC");
  private static final CQLWord MY_VALUE = CQLWord.fromInternal("My Value");
  private static final CQLWord WRITETIME = CQLWord.fromInternal("writetime");
  private static final CQLWord TTL = CQLWord.fromInternal("ttl");
  private static final CQLWord MY_WRITETIME = CQLWord.fromInternal("My Writetime");
  private static final CQLWord MY_TTL = CQLWord.fromInternal("My TTL");
  private static final CQLWord T_1 = CQLWord.fromInternal("t1");
  private static final CQLWord T_2 = CQLWord.fromInternal("t2");

  private static final CQLLiteral _16 = new CQLLiteral("16");
  private static final CQLLiteral _2 = new CQLLiteral("2");
  private static final CQLLiteral _3 = new CQLLiteral("3");

  @Test
  void should_detect_table_name_simple_insert() {
    QueryInspector inspector =
        new QueryInspector("INSERT INTO table1 (pk, cc, v) VALUES (:pk, :cc, :v)");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(TABLE_1);
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
        new QueryInspector("INSERT INTO ks.table1 (pk, cc, v) VALUES (:pk, :cc, :v)");
    assertThat(inspector.getKeyspaceName()).hasValue(KS);
    assertThat(inspector.getTableName()).isEqualTo(TABLE_1);
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
    QueryInspector inspector = new QueryInspector("UPDATE table1 SET v = ? WHERE pk = ?");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(TABLE_1);
  }

  @Test
  void should_detect_table_name_quoted_update() {
    QueryInspector inspector = new QueryInspector("UPDATE \"MyTable\" SET v = ? WHERE pk = ?");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(MY_TABLE);
  }

  @Test
  void should_detect_keyspace_and_table_name_simple_update() {
    QueryInspector inspector = new QueryInspector("UPDATE ks.table1 SET v = ? WHERE pk = ?");
    assertThat(inspector.getKeyspaceName()).hasValue(KS);
    assertThat(inspector.getTableName()).isEqualTo(TABLE_1);
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
    QueryInspector inspector = new QueryInspector("SELECT col1 FROM table1");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(TABLE_1);
  }

  @Test
  void should_detect_table_name_quoted_select() {
    QueryInspector inspector = new QueryInspector("SELECT col1 FROM \"MyTable\"");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(MY_TABLE);
  }

  @Test
  void should_detect_keyspace_and_table_name_simple_select() {
    QueryInspector inspector = new QueryInspector("SELECT col1 FROM ks.table1");
    assertThat(inspector.getKeyspaceName()).hasValue(KS);
    assertThat(inspector.getTableName()).isEqualTo(TABLE_1);
  }

  @Test
  void should_detect_keyspace_and_table_name_quoted_select() {
    QueryInspector inspector = new QueryInspector("SELECT col1 FROM \"MyKeyspace\".\"MyTable\"");
    assertThat(inspector.getKeyspaceName()).hasValue(MY_KEYSPACE);
    assertThat(inspector.getTableName()).isEqualTo(MY_TABLE);
  }

  @Test
  void should_detect_table_name_simple_delete() {
    QueryInspector inspector = new QueryInspector("DELETE col1 FROM table1 WHERE pk = 1");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(TABLE_1);
  }

  @Test
  void should_detect_table_name_quoted_delete() {
    QueryInspector inspector = new QueryInspector("DELETE col1 FROM \"MyTable\" WHERE pk = 1");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(MY_TABLE);
  }

  @Test
  void should_detect_keyspace_and_table_name_simple_delete() {
    QueryInspector inspector = new QueryInspector("DELETE col1 FROM ks.table1 WHERE pk = 1");
    assertThat(inspector.getKeyspaceName()).hasValue(KS);
    assertThat(inspector.getTableName()).isEqualTo(TABLE_1);
  }

  @Test
  void should_detect_keyspace_and_table_name_quoted_delete() {
    QueryInspector inspector =
        new QueryInspector("DELETE col1 FROM \"MyKeyspace\".\"MyTable\" WHERE pk = 1");
    assertThat(inspector.getKeyspaceName()).hasValue(MY_KEYSPACE);
    assertThat(inspector.getTableName()).isEqualTo(MY_TABLE);
  }

  @Test
  void should_detect_named_variable_insert() {
    QueryInspector inspector =
        new QueryInspector("INSERT INTO ks.table1 (pk, cc, v) VALUES (:pk, :cc, :v)");
    assertThat(inspector.getAssignments())
        .containsEntry(PK, PK)
        .containsEntry(CC, CC)
        .containsEntry(V, V);
  }

  @Test
  void should_detect_named_variable_quoted_insert() {
    QueryInspector inspector =
        new QueryInspector(
            "INSERT INTO ks.table1 (\"My PK\", \"My CC\", \"My Value\") VALUES (:\"My PK\", :\"My CC\", :\"My Value\")");
    assertThat(inspector.getAssignments())
        .containsEntry(MY_PK, MY_PK)
        .containsEntry(MY_CC, MY_CC)
        .containsEntry(MY_VALUE, MY_VALUE);
  }

  @Test
  void should_detect_positional_variable_insert() {
    QueryInspector inspector =
        new QueryInspector("INSERT INTO ks.table1 (pk, cc, v) VALUES (?,?,?)");
    assertThat(inspector.getAssignments())
        .containsEntry(PK, PK)
        .containsEntry(CC, CC)
        .containsEntry(V, V);
  }

  @Test
  void should_detect_function_variable_insert() {
    QueryInspector inspector =
        new QueryInspector("INSERT INTO ks.table1 (pk, cc, v) VALUES (?,?,now())");
    assertThat(inspector.getAssignments())
        .containsEntry(PK, PK)
        .containsEntry(CC, CC)
        .containsEntry(V, FUNC_NOW);
  }

  @Test
  void should_detect_named_variable_update() {
    QueryInspector inspector =
        new QueryInspector("UPDATE ks.table1 SET v = v + :v WHERE pk = :pk AND cc = :cc");
    assertThat(inspector.getAssignments())
        .containsEntry(PK, PK)
        .containsEntry(CC, CC)
        .containsEntry(V, V);
  }

  @Test
  void should_detect_named_variable_update_shorthand_notation() {
    QueryInspector inspector =
        new QueryInspector("UPDATE ks.table1 SET v += :v WHERE pk = :pk AND cc = :cc");
    assertThat(inspector.getAssignments())
        .containsEntry(PK, PK)
        .containsEntry(CC, CC)
        .containsEntry(V, V);
  }

  @Test
  void should_detect_named_variable_quoted_update() {
    QueryInspector inspector =
        new QueryInspector(
            "UPDATE ks.table1 SET \"My Value\" = \"My Value\" + :\"My Value\" "
                + "WHERE \"My PK\" = :\"My PK\" AND \"My CC\" = :\"My CC\"");
    assertThat(inspector.getAssignments())
        .containsEntry(MY_PK, MY_PK)
        .containsEntry(MY_CC, MY_CC)
        .containsEntry(MY_VALUE, MY_VALUE);
  }

  @Test
  void should_detect_positional_variable_update() {
    QueryInspector inspector =
        new QueryInspector("UPDATE ks.table1 SET v = v + ? WHERE pk = ? AND cc = ?");
    assertThat(inspector.getAssignments())
        .containsEntry(PK, PK)
        .containsEntry(CC, CC)
        .containsEntry(V, V);
  }

  @Test
  void should_detect_function_variable_update() {
    QueryInspector inspector =
        new QueryInspector("UPDATE ks.table1 SET v = v + now() WHERE pk = ? AND cc = ?");
    assertThat(inspector.getAssignments())
        .containsEntry(PK, PK)
        .containsEntry(CC, CC)
        .containsEntry(V, FUNC_NOW);
  }

  @Test
  void should_detect_named_variable_delete() {
    QueryInspector inspector =
        new QueryInspector("DELETE FROM ks.table1 WHERE pk = :pk AND cc = :cc");
    assertThat(inspector.getAssignments()).containsEntry(PK, PK).containsEntry(CC, CC);
  }

  @Test
  void should_detect_named_variable_quoted_delete() {
    QueryInspector inspector =
        new QueryInspector(
            "DELETE FROM ks.table1 WHERE \"My PK\" = :\"My PK\" AND \"My CC\" = :\"My CC\"");
    assertThat(inspector.getAssignments()).containsEntry(MY_PK, MY_PK).containsEntry(MY_CC, MY_CC);
  }

  @Test
  void should_detect_positional_variable_delete() {
    QueryInspector inspector = new QueryInspector("DELETE FROM ks.table1 WHERE pk = ? AND cc = ?");
    assertThat(inspector.getAssignments()).containsEntry(PK, PK).containsEntry(CC, CC);
  }

  @Test
  void should_detect_function_variable_delete() {
    QueryInspector inspector =
        new QueryInspector("DELETE FROM ks.table1 WHERE pk = ? AND cc = now()");
    assertThat(inspector.getAssignments()).containsEntry(PK, PK).containsEntry(CC, FUNC_NOW);
  }

  @Test
  void should_detect_named_variable_select() {
    QueryInspector inspector = new QueryInspector("SELECT pk, cc, v FROM ks.table1");
    assertThat(inspector.getResultSetVariables().keySet()).contains(PK, CC, V);
    assertThat(inspector.getResultSetVariables().values()).contains(PK, CC, V);
  }

  @Test
  void should_detect_named_variable_quoted_select() {
    QueryInspector inspector =
        new QueryInspector("SELECT \"My PK\", \"My CC\",\"My Value\" FROM ks.table1");
    assertThat(inspector.getResultSetVariables().keySet()).contains(MY_PK, MY_CC, MY_VALUE);
    assertThat(inspector.getResultSetVariables().values()).contains(MY_PK, MY_CC, MY_VALUE);
  }

  @Test
  void should_detect_named_variable_aliased_select() {
    QueryInspector inspector =
        new QueryInspector(
            "SELECT pk AS \"My PK\", cc AS \"My CC\", v AS \"My Value\" FROM ks.table1");
    assertThat(inspector.getResultSetVariables().keySet()).contains(PK, CC, V);
    assertThat(inspector.getResultSetVariables().values()).contains(MY_PK, MY_CC, MY_VALUE);
  }

  @Test
  void should_detect_writetime_insert() {
    QueryInspector inspector =
        new QueryInspector(
            "INSERT INTO ks.table1 (pk, cc, v) VALUES (?, ?, ?) USING TTL :ttl AND TIMESTAMP :writetime");
    assertThat(inspector.getWriteTimeVariables()).hasSize(1).containsOnly(WRITETIME);
    assertThat(inspector.getUsingTimestampVariable()).contains(WRITETIME);
    assertThat(inspector.getUsingTTLVariable()).contains(TTL);
  }

  @Test
  void should_detect_writetime_quoted_insert() {
    QueryInspector inspector =
        new QueryInspector(
            "INSERT INTO ks.table1 (pk, cc, v) VALUES (?, ?, ?) "
                + "USING TTL :\"My TTL\" AND TIMESTAMP :\"My Writetime\"");
    assertThat(inspector.getWriteTimeVariables()).hasSize(1).containsOnly(MY_WRITETIME);
    assertThat(inspector.getUsingTimestampVariable()).contains(MY_WRITETIME);
    assertThat(inspector.getUsingTTLVariable()).contains(MY_TTL);
  }

  @Test
  void should_detect_writetime_update() {
    QueryInspector inspector =
        new QueryInspector(
            "UPDATE ks.table1 USING TTL :ttl AND TIMESTAMP :writetime SET foo = :bar WHERE pk = 1");
    assertThat(inspector.getWriteTimeVariables()).hasSize(1).containsOnly(WRITETIME);
    assertThat(inspector.getUsingTimestampVariable()).contains(WRITETIME);
    assertThat(inspector.getUsingTTLVariable()).contains(TTL);
  }

  @Test
  void should_detect_writetime_quoted_update() {
    QueryInspector inspector =
        new QueryInspector(
            "UPDATE ks.table1 USING TTL :\"My TTL\" AND TiMeStAmP :\"My Writetime\" SET foo = :bar WHERE pk = 1");
    assertThat(inspector.getWriteTimeVariables()).hasSize(1).containsOnly(MY_WRITETIME);
    assertThat(inspector.getUsingTimestampVariable()).contains(MY_WRITETIME);
    assertThat(inspector.getUsingTTLVariable()).contains(MY_TTL);
  }

  @Test
  void should_detect_writetime_delete() {
    QueryInspector inspector =
        new QueryInspector("DELETE FROM ks.table1 USING TIMESTAMP :writetime WHERE pk = 1");
    assertThat(inspector.getWriteTimeVariables()).hasSize(1).containsOnly(WRITETIME);
    assertThat(inspector.getUsingTimestampVariable()).contains(WRITETIME);
  }

  @Test
  void should_detect_writetime_quoted_delete() {
    QueryInspector inspector =
        new QueryInspector("DELETE FROM ks.table1 USING TiMeStAmP :\"My Writetime\" WHERE pk = 1");
    assertThat(inspector.getWriteTimeVariables()).hasSize(1).containsOnly(MY_WRITETIME);
    assertThat(inspector.getUsingTimestampVariable()).contains(MY_WRITETIME);
  }

  @Test
  void should_detect_writetime_insert_positional() {
    QueryInspector inspector =
        new QueryInspector(
            "INSERT INTO ks.table1 (pk, cc, v) VALUES (?, ?, ?) USING TTL ? AND TIMESTAMP ?");
    assertThat(inspector.getWriteTimeVariables())
        .hasSize(1)
        .containsOnly(INTERNAL_TIMESTAMP_VARNAME);
    assertThat(inspector.getUsingTimestampVariable()).contains(INTERNAL_TIMESTAMP_VARNAME);
    assertThat(inspector.getUsingTTLVariable()).contains(INTERNAL_TTL_VARNAME);
  }

  @Test
  void should_detect_writetime_update_positional() {
    QueryInspector inspector =
        new QueryInspector("UPDATE ks.table1 USING TTL ? AND TIMESTAMP ? SET foo = ? WHERE pk = 1");
    assertThat(inspector.getWriteTimeVariables())
        .hasSize(1)
        .containsOnly(INTERNAL_TIMESTAMP_VARNAME);
    assertThat(inspector.getUsingTimestampVariable()).contains(INTERNAL_TIMESTAMP_VARNAME);
    assertThat(inspector.getUsingTTLVariable()).contains(INTERNAL_TTL_VARNAME);
  }

  @Test
  void should_detect_writetime_delete_positional() {
    QueryInspector inspector =
        new QueryInspector("DELETE FROM ks.table1 USING TIMESTAMP ? WHERE pk = 1");
    assertThat(inspector.getWriteTimeVariables())
        .hasSize(1)
        .containsOnly(INTERNAL_TIMESTAMP_VARNAME);
    assertThat(inspector.getUsingTimestampVariable()).contains(INTERNAL_TIMESTAMP_VARNAME);
  }

  @Test
  void should_detect_writetime_select() {
    QueryInspector inspector = new QueryInspector("SELECT WrItEtImE(col1) FROM ks.table1");
    assertThat(inspector.getWriteTimeVariables())
        .hasSize(1)
        .containsOnly(new FunctionCall(null, WRITETIME, COL_1));
  }

  @Test
  void should_detect_writetime_quoted_select() {
    QueryInspector inspector = new QueryInspector("SELECT WrItEtImE(\"My Col 2\") FROM ks.table1");
    assertThat(inspector.getWriteTimeVariables())
        .hasSize(1)
        .containsOnly(new FunctionCall(null, WRITETIME, MY_COL_2));
  }

  @Test
  void should_detect_writetime_select_aliased() {
    QueryInspector inspector =
        new QueryInspector("SELECT WrItEtImE(mycol) AS WRITETIME FROM ks.table1");
    assertThat(inspector.getWriteTimeVariables()).hasSize(1).containsOnly(WRITETIME);
  }

  @Test
  void should_detect_writetime_quoted_select_aliased() {
    QueryInspector inspector =
        new QueryInspector("SELECT WrItEtImE(\"My Col\") AS \"My Writetime\" FROM ks.table1");
    assertThat(inspector.getWriteTimeVariables()).hasSize(1).containsOnly(MY_WRITETIME);
  }

  @Test
  void should_detect_multiple_writetime_select() {
    QueryInspector inspector =
        new QueryInspector("SELECT writetime(col1) as t1, writetime(col2) as t2 FROM ks.table1");
    assertThat(inspector.getWriteTimeVariables()).hasSize(2).containsExactly(T_1, T_2);
  }

  @Test
  void should_detect_ttl_select() {
    QueryInspector inspector =
        new QueryInspector("SELECT TTL(col1) as t1, ttl(\"My Col 2\") FROM ks.table1");
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
            "SELECT myFunction(col1) as f1, \"MyFunction\"(\"My Col 2\") FROM ks.table1");
    CQLWord name1 = CQLWord.fromInternal("myfunction");
    CQLWord name2 = CQLWord.fromInternal("MyFunction");
    FunctionCall f1 = new FunctionCall(null, name1, COL_1);
    FunctionCall f2 = new FunctionCall(null, name2, MY_COL_2);
    assertThat(inspector.getResultSetVariables())
        .hasSize(2)
        .containsKeys(f1, f2)
        .containsValues(CQLWord.fromInternal("f1"), f2);
  }

  @Test
  void should_error_out_if_syntax_error() {
    assertThatThrownBy(() -> new QueryInspector(" not a valid statement "))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid query: ' not a valid statement ' could not be parsed at line");
  }

  @Test
  void should_error_out_if_insert_json() {
    assertThatThrownBy(() -> new QueryInspector("INSERT INTO table1 JSON '{ \"col1\" : value }'"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid query: INSERT JSON is not supported:");
  }

  @Test
  void should_error_out_if_select_json() {
    assertThatThrownBy(() -> new QueryInspector("SELECT JSON col1 FROM table1"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Invalid query: SELECT JSON is not supported:");
  }

  @Test
  void should_error_out_if_insert_values_mismatch() {
    assertThatThrownBy(() -> new QueryInspector("INSERT INTO ks.table1 (pk, cc, v) VALUES (?, ?)"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid query: the number of columns to insert (3) does not match the number of terms (2)");
    assertThatThrownBy(
            () -> new QueryInspector("INSERT INTO ks.table1 (pk, cc, v) VALUES (:pk, :cc)"))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Invalid query: the number of columns to insert (3) does not match the number of terms (2)");
  }

  @Test
  void should_detect_from_clause_start_index() {
    QueryInspector inspector = new QueryInspector("SELECT col1  \n\t   FROM ks.table1");
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
        .contains(CQLWord.fromInternal("begin"));
    assertThat(inspector.getTokenRangeRestrictionEndVariable())
        .contains(CQLWord.fromInternal("finish"));
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
        .contains(CQLWord.fromInternal("partition key token"));
    assertThat(inspector.getTokenRangeRestrictionEndVariable())
        .contains(CQLWord.fromInternal("partition key token"));
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
        arguments("SELECT CAST (col1 as int) FROM ks.t1", true),
        arguments("SELECT (int) 1 FROM ks.t1", true),
        arguments("SELECT col1, writetime(col1), ttl(col1), now() FROM ks.t1", false),
        arguments(
            "SELECT col1, writetime(col1) AS wrt, ttl(col1) as ttl, now() as now FROM ks.t1",
            false),
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
            "INSERT INTO table1 (pk, v) VALUES (0, \"MyKeyspace\".sqrt(16))",
            2,
            new FunctionCall(MY_KEYSPACE, SQRT, _16)),
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
            "UPDATE table1 set v = \"MyKeyspace\".sqrt(16) WHERE pk = 0",
            2,
            new FunctionCall(MY_KEYSPACE, SQRT, _16)),
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
        arguments("SELECT sqrt(col1) FROM table1", new FunctionCall(null, SQRT, COL_1)),
        arguments(
            "SELECT max(col1, \"My Col 2\")  FROM table1",
            new FunctionCall(null, MAX, COL_1, MY_COL_2)),
        // SELECT, qualified
        arguments("SELECT ks.now() FROM table1", new FunctionCall(KS, NOW)),
        arguments(
            "SELECT \"MyKeyspace\".sqrt(col1) FROM table1",
            new FunctionCall(MY_KEYSPACE, SQRT, COL_1)),
        arguments(
            "SELECT ks.max(col1, \"My Col 2\")  FROM table1",
            new FunctionCall(KS, MAX, COL_1, MY_COL_2)));
  }

  @ParameterizedTest
  @MethodSource
  void should_detect_solr_query(String query, boolean expected) {
    QueryInspector inspector = new QueryInspector(query);
    assertThat(inspector.hasSearchClause()).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_detect_solr_query() {
    return Lists.newArrayList(
        arguments("INSERT INTO table1 (pk, v) VALUES (1, 2)", false),
        arguments("UPDATE table1 SET v = 1 WHERE pk = 0", false),
        arguments("DELETE FROM table1 WHERE pk = 0", false),
        arguments("SELECT * FROM table1 WHERE pk = 0", false),
        arguments("SELECT * FROM table1 WHERE solr_query = 'irrelevant'", true));
  }

  @ParameterizedTest
  @MethodSource
  void should_detect_parallelizable_query(String query, boolean expected) {
    QueryInspector inspector = new QueryInspector(query);
    assertThat(inspector.isParallelizable()).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_detect_parallelizable_query() {
    return Lists.newArrayList(
        arguments("INSERT INTO table1 (pk, v) VALUES (1, 2)", true),
        arguments("UPDATE table1 SET v = 1 WHERE pk = 0", true),
        arguments("DELETE FROM table1 WHERE pk = 0", true),
        arguments("SELECT * FROM table1", true),
        arguments("SELECT * FROM table1 ALLOW FILTERING", true),
        arguments("SELECT * FROM table1 WHERE pk = 0", false),
        arguments("SELECT * FROM table1 ORDER BY foo", false),
        arguments("SELECT * FROM table1 GROUP BY foo", false),
        arguments("SELECT * FROM table1 LIMIT 10", false),
        arguments("SELECT * FROM table1 PER PARTITION LIMIT 10 ALLOW FILTERING", true),
        arguments("SELECT * FROM table1 PER PARTITION LIMIT 10 LIMIT 100", false));
  }

  @ParameterizedTest
  @MethodSource
  void should_detect_batch(String query, boolean expected) {
    QueryInspector inspector = new QueryInspector(query);
    assertThat(inspector.isBatch()).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_detect_batch() {
    return Lists.newArrayList(
        arguments("SELECT a,b,c FROM ks.t1 WHERE token(pk) > ? AND token(pk) <= ?", false),
        arguments("INSERT INTO t1 (pk,cc,v) VALUES (?,?,?)", false),
        arguments("UPDATE t1 SET v=? WHERE pk=? AND cc=?", false),
        arguments("DELETE FROM t1 WHERE pk=? AND cc=?", false),
        arguments(
            "BEGIN BATCH "
                + "INSERT INTO t1 (pk,cc,v) VALUES (?,?,?); "
                + "UPDATE t1 SET v=? WHERE pk=? AND cc=?; "
                + "DELETE FROM t1 WHERE pk=? AND cc=?; "
                + "APPLY BATCH",
            true));
  }

  @ParameterizedTest
  @MethodSource
  void should_detect_batch_child_statements(String query, List<String> expected) {
    QueryInspector inspector = new QueryInspector(query);
    assertThat(inspector.getBatchChildStatements()).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_detect_batch_child_statements() {
    return Lists.newArrayList(
        arguments("INSERT INTO t1 (pk,cc,v) VALUES (?,?,?)", Collections.emptyList()),
        arguments(
            "BEGIN BATCH "
                + "INSERT INTO t1 (pk,cc,v) VALUES (?,?,?)  ; "
                + "UPDATE t1 SET v=? WHERE pk=? AND cc=?;"
                + "DELETE FROM t1 WHERE pk=? AND cc=? "
                + "APPLY BATCH",
            ImmutableList.of(
                "INSERT INTO t1 (pk,cc,v) VALUES (?,?,?)",
                "UPDATE t1 SET v=? WHERE pk=? AND cc=?",
                "DELETE FROM t1 WHERE pk=? AND cc=?")));
  }

  @ParameterizedTest
  @MethodSource
  void should_detect_batch_type(String query, BatchType expected) {
    QueryInspector inspector = new QueryInspector(query);
    assertThat(inspector.getBatchType().orElse(null)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_detect_batch_type() {
    return Lists.newArrayList(
        arguments("INSERT INTO t1 (pk,cc,v) VALUES (?,?,?)", null),
        arguments(
            "BEGIN BATCH INSERT INTO t1 (pk,cc,v) VALUES (?,?,?) APPLY BATCH", BatchType.LOGGED),
        arguments(
            "BEGIN  UnLoGgEd BATCH INSERT INTO t1 (pk,cc,v) VALUES (?,?,?) APPLY BATCH",
            BatchType.UNLOGGED),
        arguments(
            "BEGIN  CoUnTeR  BATCH INSERT INTO t1 (pk,cc,v) VALUES (?,?,?) APPLY BATCH",
            BatchType.COUNTER));
  }

  @ParameterizedTest
  @MethodSource
  void should_detect_batch_level_using_clause(String query, boolean expected) {
    QueryInspector inspector = new QueryInspector(query);
    assertThat(inspector.hasBatchLevelUsingClause()).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_detect_batch_level_using_clause() {
    return Lists.newArrayList(
        arguments("INSERT INTO t1 (pk,cc,v) VALUES (?,?,?)", false),
        arguments("BEGIN BATCH INSERT INTO t1 (pk,cc,v) VALUES (?,?,?) APPLY BATCH", false),
        arguments(
            "BEGIN BATCH USING TTL 123 INSERT INTO t1 (pk,cc,v) VALUES (?,?,?) APPLY BATCH", true),
        arguments(
            "BEGIN BATCH INSERT INTO t1 (pk,cc,v) VALUES (?,?,?) USING TTL 123 APPLY BATCH",
            false));
  }
}
