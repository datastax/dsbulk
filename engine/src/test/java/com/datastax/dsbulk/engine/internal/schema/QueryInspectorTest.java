/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import org.junit.jupiter.api.Test;

class QueryInspectorTest {

  @Test
  void should_detect_table_name_simple_insert() {
    QueryInspector inspector =
        new QueryInspector("INSERT INTO foo (pk, cc, v) VALUES (:pk, :cc, :v)");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(CQLIdentifier.fromInternal("foo"));
  }

  @Test
  void should_detect_table_name_quoted_insert() {
    QueryInspector inspector =
        new QueryInspector("INSERT INTO \"MyTable\" (pk, cc, v) VALUES (:pk, :cc, :v)");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(CQLIdentifier.fromInternal("MyTable"));
  }

  @Test
  void should_detect_keyspace_and_table_name_simple_insert() {
    QueryInspector inspector =
        new QueryInspector("INSERT INTO ks.foo (pk, cc, v) VALUES (:pk, :cc, :v)");
    assertThat(inspector.getKeyspaceName()).hasValue(CQLIdentifier.fromInternal("ks"));
    assertThat(inspector.getTableName()).isEqualTo(CQLIdentifier.fromInternal("foo"));
  }

  @Test
  void should_detect_keyspace_and_table_name_quoted_insert() {
    QueryInspector inspector =
        new QueryInspector(
            "INSERT INTO \"MyKeyspace\".\"MyTable\" (pk, cc, v) VALUES (:pk, :cc, :v)");
    assertThat(inspector.getKeyspaceName()).hasValue(CQLIdentifier.fromInternal("MyKeyspace"));
    assertThat(inspector.getTableName()).isEqualTo(CQLIdentifier.fromInternal("MyTable"));
  }

  @Test
  void should_detect_table_name_simple_update() {
    QueryInspector inspector = new QueryInspector("UPDATE foo SET v = ? WHERE pk = ?");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(CQLIdentifier.fromInternal("foo"));
  }

  @Test
  void should_detect_table_name_quoted_update() {
    QueryInspector inspector = new QueryInspector("UPDATE \"MyTable\" SET v = ? WHERE pk = ?");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(CQLIdentifier.fromInternal("MyTable"));
  }

  @Test
  void should_detect_keyspace_and_table_name_simple_update() {
    QueryInspector inspector = new QueryInspector("UPDATE ks.foo SET v = ? WHERE pk = ?");
    assertThat(inspector.getKeyspaceName()).hasValue(CQLIdentifier.fromInternal("ks"));
    assertThat(inspector.getTableName()).isEqualTo(CQLIdentifier.fromInternal("foo"));
  }

  @Test
  void should_detect_keyspace_and_table_name_quoted_update() {
    QueryInspector inspector =
        new QueryInspector("UPDATE \"MyKeyspace\".\"MyTable\" SET v = ? WHERE pk = ?");
    assertThat(inspector.getKeyspaceName()).hasValue(CQLIdentifier.fromInternal("MyKeyspace"));
    assertThat(inspector.getTableName()).isEqualTo(CQLIdentifier.fromInternal("MyTable"));
  }

  @Test
  void should_detect_table_name_simple_select() {
    QueryInspector inspector = new QueryInspector("SELECT c1 FROM foo");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(CQLIdentifier.fromInternal("foo"));
  }

  @Test
  void should_detect_table_name_quoted_select() {
    QueryInspector inspector = new QueryInspector("SELECT c1 FROM \"MyTable\"");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(CQLIdentifier.fromInternal("MyTable"));
  }

  @Test
  void should_detect_keyspace_and_table_name_simple_select() {
    QueryInspector inspector = new QueryInspector("SELECT c1 FROM ks.foo");
    assertThat(inspector.getKeyspaceName()).hasValue(CQLIdentifier.fromInternal("ks"));
    assertThat(inspector.getTableName()).isEqualTo(CQLIdentifier.fromInternal("foo"));
  }

  @Test
  void should_detect_keyspace_and_table_name_quoted_select() {
    QueryInspector inspector = new QueryInspector("SELECT c1 FROM \"MyKeyspace\".\"MyTable\"");
    assertThat(inspector.getKeyspaceName()).hasValue(CQLIdentifier.fromInternal("MyKeyspace"));
    assertThat(inspector.getTableName()).isEqualTo(CQLIdentifier.fromInternal("MyTable"));
  }

  @Test
  void should_detect_table_name_simple_delete() {
    QueryInspector inspector = new QueryInspector("DELETE c1 FROM foo WHERE pk = 1");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(CQLIdentifier.fromInternal("foo"));
  }

  @Test
  void should_detect_table_name_quoted_delete() {
    QueryInspector inspector = new QueryInspector("DELETE c1 FROM \"MyTable\" WHERE pk = 1");
    assertThat(inspector.getKeyspaceName()).isNotPresent();
    assertThat(inspector.getTableName()).isEqualTo(CQLIdentifier.fromInternal("MyTable"));
  }

  @Test
  void should_detect_keyspace_and_table_name_simple_delete() {
    QueryInspector inspector = new QueryInspector("DELETE c1 FROM ks.foo WHERE pk = 1");
    assertThat(inspector.getKeyspaceName()).hasValue(CQLIdentifier.fromInternal("ks"));
    assertThat(inspector.getTableName()).isEqualTo(CQLIdentifier.fromInternal("foo"));
  }

  @Test
  void should_detect_keyspace_and_table_name_quoted_delete() {
    QueryInspector inspector =
        new QueryInspector("DELETE c1 FROM \"MyKeyspace\".\"MyTable\" WHERE pk = 1");
    assertThat(inspector.getKeyspaceName()).hasValue(CQLIdentifier.fromInternal("MyKeyspace"));
    assertThat(inspector.getTableName()).isEqualTo(CQLIdentifier.fromInternal("MyTable"));
  }

  @Test
  void should_detect_named_variable_insert() {
    QueryInspector inspector =
        new QueryInspector("INSERT INTO ks.foo (pk, cc, v) VALUES (:pk, :cc, :v)");
    assertThat(inspector.getAssignments())
        .containsEntry(CQLIdentifier.fromInternal("pk"), CQLIdentifier.fromInternal("pk"))
        .containsEntry(CQLIdentifier.fromInternal("cc"), CQLIdentifier.fromInternal("cc"))
        .containsEntry(CQLIdentifier.fromInternal("v"), CQLIdentifier.fromInternal("v"));
  }

  @Test
  void should_detect_named_variable_quoted_insert() {
    QueryInspector inspector =
        new QueryInspector(
            "INSERT INTO ks.foo (\"My PK\", \"My CC\", \"My Value\") VALUES (:\"My PK\", :\"My CC\", :\"My Value\")");
    assertThat(inspector.getAssignments())
        .containsEntry(CQLIdentifier.fromInternal("My PK"), CQLIdentifier.fromInternal("My PK"))
        .containsEntry(CQLIdentifier.fromInternal("My CC"), CQLIdentifier.fromInternal("My CC"))
        .containsEntry(
            CQLIdentifier.fromInternal("My Value"), CQLIdentifier.fromInternal("My Value"));
  }

  @Test
  void should_detect_positional_variable_insert() {
    QueryInspector inspector = new QueryInspector("INSERT INTO ks.foo (pk, cc, v) VALUES (?,?,?)");
    assertThat(inspector.getAssignments())
        .containsEntry(CQLIdentifier.fromInternal("pk"), CQLIdentifier.fromInternal("pk"))
        .containsEntry(CQLIdentifier.fromInternal("cc"), CQLIdentifier.fromInternal("cc"))
        .containsEntry(CQLIdentifier.fromInternal("v"), CQLIdentifier.fromInternal("v"));
  }

  @Test
  void should_detect_function_variable_insert() {
    QueryInspector inspector =
        new QueryInspector("INSERT INTO ks.foo (pk, cc, v) VALUES (?,?,now())");
    assertThat(inspector.getAssignments())
        .containsEntry(CQLIdentifier.fromInternal("pk"), CQLIdentifier.fromInternal("pk"))
        .containsEntry(CQLIdentifier.fromInternal("cc"), CQLIdentifier.fromInternal("cc"))
        .containsEntry(
            CQLIdentifier.fromInternal("v"), new FunctionCall(CQLIdentifier.fromInternal("now")));
  }

  @Test
  void should_detect_named_variable_update() {
    QueryInspector inspector =
        new QueryInspector("UPDATE ks.foo SET v = v + :v WHERE pk = :pk AND cc = :cc");
    assertThat(inspector.getAssignments())
        .containsEntry(CQLIdentifier.fromInternal("pk"), CQLIdentifier.fromInternal("pk"))
        .containsEntry(CQLIdentifier.fromInternal("cc"), CQLIdentifier.fromInternal("cc"))
        .containsEntry(CQLIdentifier.fromInternal("v"), CQLIdentifier.fromInternal("v"));
  }

  @Test
  void should_detect_named_variable_update_shorthand_notation() {
    QueryInspector inspector =
        new QueryInspector("UPDATE ks.foo SET v += :v WHERE pk = :pk AND cc = :cc");
    assertThat(inspector.getAssignments())
        .containsEntry(CQLIdentifier.fromInternal("pk"), CQLIdentifier.fromInternal("pk"))
        .containsEntry(CQLIdentifier.fromInternal("cc"), CQLIdentifier.fromInternal("cc"))
        .containsEntry(CQLIdentifier.fromInternal("v"), CQLIdentifier.fromInternal("v"));
  }

  @Test
  void should_detect_named_variable_quoted_update() {
    QueryInspector inspector =
        new QueryInspector(
            "UPDATE ks.foo SET \"My Value\" = \"My Value\" + :\"My Value\" "
                + "WHERE \"My PK\" = :\"My PK\" AND \"My CC\" = :\"My CC\"");
    assertThat(inspector.getAssignments())
        .containsEntry(CQLIdentifier.fromInternal("My PK"), CQLIdentifier.fromInternal("My PK"))
        .containsEntry(CQLIdentifier.fromInternal("My CC"), CQLIdentifier.fromInternal("My CC"))
        .containsEntry(
            CQLIdentifier.fromInternal("My Value"), CQLIdentifier.fromInternal("My Value"));
  }

  @Test
  void should_detect_positional_variable_update() {
    QueryInspector inspector =
        new QueryInspector("UPDATE ks.foo SET v = v + ? WHERE pk = ? AND cc = ?");
    assertThat(inspector.getAssignments())
        .containsEntry(CQLIdentifier.fromInternal("pk"), CQLIdentifier.fromInternal("pk"))
        .containsEntry(CQLIdentifier.fromInternal("cc"), CQLIdentifier.fromInternal("cc"))
        .containsEntry(CQLIdentifier.fromInternal("v"), CQLIdentifier.fromInternal("v"));
  }

  @Test
  void should_detect_function_variable_update() {
    QueryInspector inspector =
        new QueryInspector("UPDATE ks.foo SET v = v + now() WHERE pk = ? AND cc = ?");
    assertThat(inspector.getAssignments())
        .containsEntry(CQLIdentifier.fromInternal("pk"), CQLIdentifier.fromInternal("pk"))
        .containsEntry(CQLIdentifier.fromInternal("cc"), CQLIdentifier.fromInternal("cc"))
        .containsEntry(
            CQLIdentifier.fromInternal("v"), new FunctionCall(CQLIdentifier.fromInternal("now")));
  }

  @Test
  void should_detect_named_variable_delete() {
    QueryInspector inspector = new QueryInspector("DELETE FROM ks.foo WHERE pk = :pk AND cc = :cc");
    assertThat(inspector.getAssignments())
        .containsEntry(CQLIdentifier.fromInternal("pk"), CQLIdentifier.fromInternal("pk"))
        .containsEntry(CQLIdentifier.fromInternal("cc"), CQLIdentifier.fromInternal("cc"));
  }

  @Test
  void should_detect_named_variable_quoted_delete() {
    QueryInspector inspector =
        new QueryInspector(
            "DELETE FROM ks.foo WHERE \"My PK\" = :\"My PK\" AND \"My CC\" = :\"My CC\"");
    assertThat(inspector.getAssignments())
        .containsEntry(CQLIdentifier.fromInternal("My PK"), CQLIdentifier.fromInternal("My PK"))
        .containsEntry(CQLIdentifier.fromInternal("My CC"), CQLIdentifier.fromInternal("My CC"));
  }

  @Test
  void should_detect_positional_variable_delete() {
    QueryInspector inspector = new QueryInspector("DELETE FROM ks.foo WHERE pk = ? AND cc = ?");
    assertThat(inspector.getAssignments())
        .containsEntry(CQLIdentifier.fromInternal("pk"), CQLIdentifier.fromInternal("pk"))
        .containsEntry(CQLIdentifier.fromInternal("cc"), CQLIdentifier.fromInternal("cc"));
  }

  @Test
  void should_detect_function_variable_delete() {
    QueryInspector inspector = new QueryInspector("DELETE FROM ks.foo WHERE pk = ? AND cc = now()");
    assertThat(inspector.getAssignments())
        .containsEntry(CQLIdentifier.fromInternal("pk"), CQLIdentifier.fromInternal("pk"))
        .containsEntry(
            CQLIdentifier.fromInternal("cc"), new FunctionCall(CQLIdentifier.fromInternal("now")));
  }

  @Test
  void should_detect_named_variable_select() {
    QueryInspector inspector = new QueryInspector("SELECT pk, cc, v FROM ks.foo");
    assertThat(inspector.getResultSetVariables().keySet())
        .contains(
            CQLIdentifier.fromInternal("pk"),
            CQLIdentifier.fromInternal("cc"),
            CQLIdentifier.fromInternal("v"));
    assertThat(inspector.getResultSetVariables().values())
        .contains(
            CQLIdentifier.fromInternal("pk"),
            CQLIdentifier.fromInternal("cc"),
            CQLIdentifier.fromInternal("v"));
  }

  @Test
  void should_detect_named_variable_quoted_select() {
    QueryInspector inspector =
        new QueryInspector("SELECT \"My PK\", \"My CC\",\"My Value\" FROM ks.foo");
    assertThat(inspector.getResultSetVariables().keySet())
        .contains(
            CQLIdentifier.fromInternal("My PK"),
            CQLIdentifier.fromInternal("My CC"),
            CQLIdentifier.fromInternal("My Value"));
    assertThat(inspector.getResultSetVariables().values())
        .contains(
            CQLIdentifier.fromInternal("My PK"),
            CQLIdentifier.fromInternal("My CC"),
            CQLIdentifier.fromInternal("My Value"));
  }

  @Test
  void should_detect_named_variable_aliased_select() {
    QueryInspector inspector =
        new QueryInspector(
            "SELECT pk AS \"My PK\", cc AS \"My CC\", v AS \"My Value\" FROM ks.foo");
    assertThat(inspector.getResultSetVariables().keySet())
        .contains(
            CQLIdentifier.fromInternal("pk"),
            CQLIdentifier.fromInternal("cc"),
            CQLIdentifier.fromInternal("v"));
    assertThat(inspector.getResultSetVariables().values())
        .contains(
            CQLIdentifier.fromInternal("My PK"),
            CQLIdentifier.fromInternal("My CC"),
            CQLIdentifier.fromInternal("My Value"));
  }

  @Test
  void should_detect_writetime_insert() {
    QueryInspector inspector =
        new QueryInspector(
            "INSERT INTO ks.foo (pk, cc, v) VALUES (?, ?, ?) USING TTL :ttl AND TIMESTAMP :writetime");
    assertThat(inspector.getWriteTimeVariables())
        .hasSize(1)
        .containsOnly(CQLIdentifier.fromInternal("writetime"));
  }

  @Test
  void should_detect_writetime_quoted_insert() {
    QueryInspector inspector =
        new QueryInspector(
            "INSERT INTO ks.foo (pk, cc, v) VALUES (?, ?, ?) "
                + "USING TTL :ttl AND TIMESTAMP :\"My Writetime\"");
    assertThat(inspector.getWriteTimeVariables())
        .hasSize(1)
        .containsOnly(CQLIdentifier.fromInternal("My Writetime"));
  }

  @Test
  void should_detect_writetime_update() {
    QueryInspector inspector =
        new QueryInspector(
            "UPDATE ks.foo USING TTL :ttl AND TIMESTAMP :writetime SET foo = :bar WHERE pk = 1");
    assertThat(inspector.getWriteTimeVariables())
        .hasSize(1)
        .containsOnly(CQLIdentifier.fromInternal("writetime"));
  }

  @Test
  void should_detect_writetime_quoted_update() {
    QueryInspector inspector =
        new QueryInspector(
            "UPDATE ks.foo USING TTL :ttl AND TiMeStAmP :\"My Writetime\" SET foo = :bar WHERE pk = 1");
    assertThat(inspector.getWriteTimeVariables())
        .hasSize(1)
        .containsOnly(CQLIdentifier.fromInternal("My Writetime"));
  }

  @Test
  void should_detect_writetime_delete() {
    QueryInspector inspector =
        new QueryInspector("DELETE FROM ks.foo USING TIMESTAMP :writetime WHERE pk = 1");
    assertThat(inspector.getWriteTimeVariables())
        .hasSize(1)
        .containsOnly(CQLIdentifier.fromInternal("writetime"));
  }

  @Test
  void should_detect_writetime_quoted_delete() {
    QueryInspector inspector =
        new QueryInspector("DELETE FROM ks.foo USING TiMeStAmP :\"My Writetime\" WHERE pk = 1");
    assertThat(inspector.getWriteTimeVariables())
        .hasSize(1)
        .containsOnly(CQLIdentifier.fromInternal("My Writetime"));
  }

  @Test
  void should_detect_writetime_select() {
    QueryInspector inspector = new QueryInspector("SELECT WrItEtImE(mycol) FROM ks.foo");
    assertThat(inspector.getWriteTimeVariables())
        .hasSize(1)
        .containsOnly(
            new FunctionCall(
                CQLIdentifier.fromInternal("writetime"), CQLIdentifier.fromInternal("mycol")));
  }

  @Test
  void should_detect_writetime_quoted_select() {
    QueryInspector inspector = new QueryInspector("SELECT WrItEtImE(\"My Col\") FROM ks.foo");
    assertThat(inspector.getWriteTimeVariables())
        .hasSize(1)
        .containsOnly(
            new FunctionCall(
                CQLIdentifier.fromInternal("writetime"), CQLIdentifier.fromInternal("My Col")));
  }

  @Test
  void should_detect_writetime_select_aliased() {
    QueryInspector inspector =
        new QueryInspector("SELECT WrItEtImE(mycol) AS WRITETIME FROM ks.foo");
    assertThat(inspector.getWriteTimeVariables())
        .hasSize(1)
        .containsOnly(CQLIdentifier.fromInternal("writetime"));
  }

  @Test
  void should_detect_writetime_quoted_select_aliased() {
    QueryInspector inspector =
        new QueryInspector("SELECT WrItEtImE(\"My Col\") AS \"My Writetime\" FROM ks.foo");
    assertThat(inspector.getWriteTimeVariables())
        .hasSize(1)
        .containsOnly(CQLIdentifier.fromInternal("My Writetime"));
  }

  @Test
  void should_detect_multiple_writetime_select() {
    QueryInspector inspector =
        new QueryInspector("SELECT writetime(col1) as t1, writetime(col2) as t2 FROM ks.foo");
    assertThat(inspector.getWriteTimeVariables())
        .hasSize(2)
        .containsExactly(CQLIdentifier.fromInternal("t1"), CQLIdentifier.fromInternal("t2"));
  }

  @Test
  void should_detect_ttl_select() {
    QueryInspector inspector =
        new QueryInspector("SELECT TTL(col1) as t1, ttl(\"My Col 2\") FROM ks.foo");
    CQLIdentifier name = CQLIdentifier.fromInternal("ttl");
    FunctionCall ttl1 = new FunctionCall(name, CQLIdentifier.fromInternal("col1"));
    FunctionCall ttl2 = new FunctionCall(name, CQLIdentifier.fromInternal("My Col 2"));
    assertThat(inspector.getResultSetVariables())
        .hasSize(2)
        .containsKeys(ttl1, ttl2)
        .containsValues(CQLIdentifier.fromInternal("t1"), ttl2);
  }

  @Test
  void should_detect_function_select() {
    QueryInspector inspector =
        new QueryInspector(
            "SELECT myFunction(col1) as f1, \"MyFunction\"(\"My Col 2\") FROM ks.foo");
    CQLIdentifier name1 = CQLIdentifier.fromInternal("myfunction");
    CQLIdentifier name2 = CQLIdentifier.fromInternal("MyFunction");
    FunctionCall f1 = new FunctionCall(name1, CQLIdentifier.fromInternal("col1"));
    FunctionCall f2 = new FunctionCall(name2, CQLIdentifier.fromInternal("My Col 2"));
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
  void should_error_out_if_writetime_positional() {
    assertThatThrownBy(
            () ->
                new QueryInspector(
                    "INSERT INTO ks.foo (pk, cc, v) VALUES (?, ?, ?) USING TIMESTAMP ?"))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid query: positional variables are not allowed in USING TIMESTAMP clauses");
  }

  @Test
  void should_error_out_if_ttl_positional() {
    assertThatThrownBy(
            () -> new QueryInspector("INSERT INTO ks.foo (pk, cc, v) VALUES (?, ?, ?) USING TTL ?"))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid query: positional variables are not allowed in USING TTL clauses");
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

  @Test
  void should_report_named_token_range_restriction_variables() {
    QueryInspector inspector =
        new QueryInspector(
            "SELECT a,b,c FROM ks.t1 WHERE token(pk) <= :finish AND token(pk) > :\"begin\"");
    assertThat(inspector.getTokenRangeRestrictionStartVariable())
        .contains(CQLIdentifier.fromInternal("begin"));
    assertThat(inspector.getTokenRangeRestrictionEndVariable())
        .contains(CQLIdentifier.fromInternal("finish"));
  }

  @Test
  void should_report_positional_token_range_restriction_variables() {
    assertThatThrownBy(
            () ->
                new QueryInspector(
                    "SELECT a,b,c FROM ks.t1 WHERE token(pk) <= ? AND token(pk) > ?"))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Invalid query: positional variables are not allowed in WHERE token(...) clauses, "
                + "please une named variables instead: "
                + "SELECT a,b,c FROM ks.t1 WHERE token(pk) <= ? AND token(pk) > ?.");
  }
}
