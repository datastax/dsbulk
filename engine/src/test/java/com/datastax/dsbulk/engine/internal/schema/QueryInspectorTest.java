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
import com.datastax.oss.driver.api.core.CqlIdentifier;
import org.junit.jupiter.api.Test;

class QueryInspectorTest {

  @Test
  void should_detect_table_name_simple_insert() {
    QueryInspector inspector =
        new QueryInspector("INSERT INTO foo (pk, cc, v) VALUES (:pk, :cc, :v)");
    assertThat(inspector.getKeyspaceName()).isNull();
    assertThat(inspector.getTableName().asInternal()).isEqualTo("foo");
  }

  @Test
  void should_detect_table_name_quoted_insert() {
    QueryInspector inspector =
        new QueryInspector("INSERT INTO \"MyTable\" (pk, cc, v) VALUES (:pk, :cc, :v)");
    assertThat(inspector.getKeyspaceName()).isNull();
    assertThat(inspector.getTableName().asInternal()).isEqualTo("MyTable");
  }

  @Test
  void should_detect_keyspace_and_table_name_simple_insert() {
    QueryInspector inspector =
        new QueryInspector("INSERT INTO ks.foo (pk, cc, v) VALUES (:pk, :cc, :v)");
    assertThat(inspector.getKeyspaceName().asInternal()).isEqualTo("ks");
    assertThat(inspector.getTableName().asInternal()).isEqualTo("foo");
  }

  @Test
  void should_detect_keyspace_and_table_name_quoted_insert() {
    QueryInspector inspector =
        new QueryInspector(
            "INSERT INTO \"MyKeyspace\".\"MyTable\" (pk, cc, v) VALUES (:pk, :cc, :v)");
    assertThat(inspector.getKeyspaceName().asInternal()).isEqualTo("MyKeyspace");
    assertThat(inspector.getTableName().asInternal()).isEqualTo("MyTable");
  }

  @Test
  void should_detect_table_name_simple_update() {
    QueryInspector inspector = new QueryInspector("UPDATE foo SET v = ? WHERE pk = ?");
    assertThat(inspector.getKeyspaceName()).isNull();
    assertThat(inspector.getTableName().asInternal()).isEqualTo("foo");
  }

  @Test
  void should_detect_table_name_quoted_update() {
    QueryInspector inspector = new QueryInspector("UPDATE \"MyTable\" SET v = ? WHERE pk = ?");
    assertThat(inspector.getKeyspaceName()).isNull();
    assertThat(inspector.getTableName().asInternal()).isEqualTo("MyTable");
  }

  @Test
  void should_detect_keyspace_and_table_name_simple_update() {
    QueryInspector inspector = new QueryInspector("UPDATE ks.foo SET v = ? WHERE pk = ?");
    assertThat(inspector.getKeyspaceName().asInternal()).isEqualTo("ks");
    assertThat(inspector.getTableName().asInternal()).isEqualTo("foo");
  }

  @Test
  void should_detect_keyspace_and_table_name_quoted_update() {
    QueryInspector inspector =
        new QueryInspector("UPDATE \"MyKeyspace\".\"MyTable\" SET v = ? WHERE pk = ?");
    assertThat(inspector.getKeyspaceName().asInternal()).isEqualTo("MyKeyspace");
    assertThat(inspector.getTableName().asInternal()).isEqualTo("MyTable");
  }

  @Test
  void should_detect_table_name_simple_select() {
    QueryInspector inspector = new QueryInspector("SELECT c1 FROM foo");
    assertThat(inspector.getKeyspaceName()).isNull();
    assertThat(inspector.getTableName().asInternal()).isEqualTo("foo");
  }

  @Test
  void should_detect_table_name_quoted_select() {
    QueryInspector inspector = new QueryInspector("SELECT c1 FROM \"MyTable\"");
    assertThat(inspector.getKeyspaceName()).isNull();
    assertThat(inspector.getTableName().asInternal()).isEqualTo("MyTable");
  }

  @Test
  void should_detect_keyspace_and_table_name_simple_select() {
    QueryInspector inspector = new QueryInspector("SELECT c1 FROM ks.foo");
    assertThat(inspector.getKeyspaceName().asInternal()).isEqualTo("ks");
    assertThat(inspector.getTableName().asInternal()).isEqualTo("foo");
  }

  @Test
  void should_detect_keyspace_and_table_name_quoted_select() {
    QueryInspector inspector = new QueryInspector("SELECT c1 FROM \"MyKeyspace\".\"MyTable\"");
    assertThat(inspector.getKeyspaceName().asInternal()).isEqualTo("MyKeyspace");
    assertThat(inspector.getTableName().asInternal()).isEqualTo("MyTable");
  }

  @Test
  void should_detect_table_name_simple_delete() {
    QueryInspector inspector = new QueryInspector("DELETE c1 FROM foo WHERE pk = 1");
    assertThat(inspector.getKeyspaceName()).isNull();
    assertThat(inspector.getTableName().asInternal()).isEqualTo("foo");
  }

  @Test
  void should_detect_table_name_quoted_delete() {
    QueryInspector inspector = new QueryInspector("DELETE c1 FROM \"MyTable\" WHERE pk = 1");
    assertThat(inspector.getKeyspaceName()).isNull();
    assertThat(inspector.getTableName().asInternal()).isEqualTo("MyTable");
  }

  @Test
  void should_detect_keyspace_and_table_name_simple_delete() {
    QueryInspector inspector = new QueryInspector("DELETE c1 FROM ks.foo WHERE pk = 1");
    assertThat(inspector.getKeyspaceName().asInternal()).isEqualTo("ks");
    assertThat(inspector.getTableName().asInternal()).isEqualTo("foo");
  }

  @Test
  void should_detect_keyspace_and_table_name_quoted_delete() {
    QueryInspector inspector =
        new QueryInspector("DELETE c1 FROM \"MyKeyspace\".\"MyTable\" WHERE pk = 1");
    assertThat(inspector.getKeyspaceName().asInternal()).isEqualTo("MyKeyspace");
    assertThat(inspector.getTableName().asInternal()).isEqualTo("MyTable");
  }

  @Test
  void should_detect_named_variable_insert() {
    QueryInspector inspector =
        new QueryInspector("INSERT INTO ks.foo (pk, cc, v) VALUES (:pk, :cc, :v)");
    assertThat(inspector.getColumnsToVariables())
        .containsEntry(CqlIdentifier.fromInternal("pk"), "pk")
        .containsEntry(CqlIdentifier.fromInternal("cc"), "cc")
        .containsEntry(CqlIdentifier.fromInternal("v"), "v");
  }

  @Test
  void should_detect_named_variable_quoted_insert() {
    QueryInspector inspector =
        new QueryInspector(
            "INSERT INTO ks.foo (\"My PK\", \"My CC\", \"My Value\") VALUES (:\"My PK\", :\"My CC\", :\"My Value\")");
    assertThat(inspector.getColumnsToVariables())
        .containsEntry(CqlIdentifier.fromInternal("My PK"), "My PK")
        .containsEntry(CqlIdentifier.fromInternal("My CC"), "My CC")
        .containsEntry(CqlIdentifier.fromInternal("My Value"), "My Value");
  }

  @Test
  void should_detect_positional_variable_insert() {
    QueryInspector inspector = new QueryInspector("INSERT INTO ks.foo (pk, cc, v) VALUES (?,?,?)");
    assertThat(inspector.getColumnsToVariables())
        .containsEntry(CqlIdentifier.fromInternal("pk"), "pk")
        .containsEntry(CqlIdentifier.fromInternal("cc"), "cc")
        .containsEntry(CqlIdentifier.fromInternal("v"), "v");
  }

  @Test
  void should_detect_function_variable_insert() {
    QueryInspector inspector =
        new QueryInspector("INSERT INTO ks.foo (pk, cc, v) VALUES (?,?,now())");
    assertThat(inspector.getColumnsToVariables())
        .containsEntry(CqlIdentifier.fromInternal("pk"), "pk")
        .containsEntry(CqlIdentifier.fromInternal("cc"), "cc")
        .containsEntry(CqlIdentifier.fromInternal("v"), "now()");
  }

  @Test
  void should_detect_named_variable_update() {
    QueryInspector inspector =
        new QueryInspector("UPDATE ks.foo SET v = v + :v WHERE pk = :pk AND cc = :cc");
    assertThat(inspector.getColumnsToVariables())
        .containsEntry(CqlIdentifier.fromInternal("pk"), "pk")
        .containsEntry(CqlIdentifier.fromInternal("cc"), "cc")
        .containsEntry(CqlIdentifier.fromInternal("v"), "v");
  }

  @Test
  void should_detect_named_variable_quoted_update() {
    QueryInspector inspector =
        new QueryInspector(
            "UPDATE ks.foo SET \"My Value\" = \"My Value\" + :\"My Value\" "
                + "WHERE \"My PK\" = :\"My PK\" AND \"My CC\" = :\"My CC\"");
    assertThat(inspector.getColumnsToVariables())
        .containsEntry(CqlIdentifier.fromInternal("My PK"), "My PK")
        .containsEntry(CqlIdentifier.fromInternal("My CC"), "My CC")
        .containsEntry(CqlIdentifier.fromInternal("My Value"), "My Value");
  }

  @Test
  void should_detect_positional_variable_update() {
    QueryInspector inspector =
        new QueryInspector("UPDATE ks.foo SET v = v + ? WHERE pk = ? AND cc = ?");
    assertThat(inspector.getColumnsToVariables())
        .containsEntry(CqlIdentifier.fromInternal("pk"), "pk")
        .containsEntry(CqlIdentifier.fromInternal("cc"), "cc")
        .containsEntry(CqlIdentifier.fromInternal("v"), "v");
  }

  @Test
  void should_detect_function_variable_update() {
    QueryInspector inspector =
        new QueryInspector("UPDATE ks.foo SET v = v + now() WHERE pk = ? AND cc = ?");
    assertThat(inspector.getColumnsToVariables())
        .containsEntry(CqlIdentifier.fromInternal("pk"), "pk")
        .containsEntry(CqlIdentifier.fromInternal("cc"), "cc")
        .containsEntry(CqlIdentifier.fromInternal("v"), "now()");
  }

  @Test
  void should_detect_named_variable_delete() {
    QueryInspector inspector = new QueryInspector("DELETE FROM ks.foo WHERE pk = :pk AND cc = :cc");
    assertThat(inspector.getColumnsToVariables())
        .containsEntry(CqlIdentifier.fromInternal("pk"), "pk")
        .containsEntry(CqlIdentifier.fromInternal("cc"), "cc");
  }

  @Test
  void should_detect_named_variable_quoted_delete() {
    QueryInspector inspector =
        new QueryInspector(
            "DELETE FROM ks.foo WHERE \"My PK\" = :\"My PK\" AND \"My CC\" = :\"My CC\"");
    assertThat(inspector.getColumnsToVariables())
        .containsEntry(CqlIdentifier.fromInternal("My PK"), "My PK")
        .containsEntry(CqlIdentifier.fromInternal("My CC"), "My CC");
  }

  @Test
  void should_detect_positional_variable_delete() {
    QueryInspector inspector = new QueryInspector("DELETE FROM ks.foo WHERE pk = ? AND cc = ?");
    assertThat(inspector.getColumnsToVariables())
        .containsEntry(CqlIdentifier.fromInternal("pk"), "pk")
        .containsEntry(CqlIdentifier.fromInternal("cc"), "cc");
  }

  @Test
  void should_detect_function_variable_delete() {
    QueryInspector inspector = new QueryInspector("DELETE FROM ks.foo WHERE pk = ? AND cc = now()");
    assertThat(inspector.getColumnsToVariables())
        .containsEntry(CqlIdentifier.fromInternal("pk"), "pk")
        .containsEntry(CqlIdentifier.fromInternal("cc"), "now()");
  }

  @Test
  void should_detect_named_variable_select() {
    QueryInspector inspector = new QueryInspector("SELECT pk, cc, v FROM ks.foo");
    assertThat(inspector.getColumnsToVariables())
        .containsEntry(CqlIdentifier.fromInternal("pk"), "pk")
        .containsEntry(CqlIdentifier.fromInternal("cc"), "cc")
        .containsEntry(CqlIdentifier.fromInternal("v"), "v");
  }

  @Test
  void should_detect_named_variable_quoted_select() {
    QueryInspector inspector =
        new QueryInspector("SELECT \"My PK\", \"My CC\",\"My Value\" FROM ks.foo");
    assertThat(inspector.getColumnsToVariables())
        .containsEntry(CqlIdentifier.fromInternal("My PK"), "My PK")
        .containsEntry(CqlIdentifier.fromInternal("My CC"), "My CC")
        .containsEntry(CqlIdentifier.fromInternal("My Value"), "My Value");
  }

  @Test
  void should_detect_named_variable_aliased_select() {
    QueryInspector inspector =
        new QueryInspector(
            "SELECT pk AS \"My PK\", cc AS \"My CC\", v AS \"My Value\" FROM ks.foo");
    assertThat(inspector.getColumnsToVariables())
        .containsEntry(CqlIdentifier.fromInternal("pk"), "My PK")
        .containsEntry(CqlIdentifier.fromInternal("cc"), "My CC")
        .containsEntry(CqlIdentifier.fromInternal("v"), "My Value");
  }

  @Test
  void should_detect_writetime_insert() {
    QueryInspector inspector =
        new QueryInspector(
            "INSERT INTO ks.foo (pk, cc, v) VALUES (?, ?, ?) USING TTL :ttl AND TIMESTAMP :writetime");
    assertThat(inspector.getWriteTimeVariable().asInternal()).isEqualTo("writetime");
  }

  @Test
  void should_detect_writetime_quoted_insert() {
    QueryInspector inspector =
        new QueryInspector(
            "INSERT INTO ks.foo (pk, cc, v) VALUES (?, ?, ?) "
                + "USING TTL :ttl AND TIMESTAMP :\"My Writetime\"");
    assertThat(inspector.getWriteTimeVariable().asInternal()).isEqualTo("My Writetime");
  }

  @Test
  void should_detect_writetime_update() {
    QueryInspector inspector =
        new QueryInspector(
            "UPDATE ks.foo USING TTL :ttl AND TIMESTAMP :writetime SET foo = :bar WHERE pk = 1");
    assertThat(inspector.getWriteTimeVariable().asInternal()).isEqualTo("writetime");
  }

  @Test
  void should_detect_writetime_quoted_update() {
    QueryInspector inspector =
        new QueryInspector(
            "UPDATE ks.foo USING TTL :ttl AND TiMeStAmP :\"My Writetime\" SET foo = :bar WHERE pk = 1");
    assertThat(inspector.getWriteTimeVariable().asInternal()).isEqualTo("My Writetime");
  }

  @Test
  void should_detect_writetime_delete() {
    QueryInspector inspector =
        new QueryInspector("DELETE FROM ks.foo USING TIMESTAMP :writetime WHERE pk = 1");
    assertThat(inspector.getWriteTimeVariable().asInternal()).isEqualTo("writetime");
  }

  @Test
  void should_detect_writetime_quoted_delete() {
    QueryInspector inspector =
        new QueryInspector("DELETE FROM ks.foo USING TiMeStAmP :\"My Writetime\" WHERE pk = 1");
    assertThat(inspector.getWriteTimeVariable().asInternal()).isEqualTo("My Writetime");
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
}
