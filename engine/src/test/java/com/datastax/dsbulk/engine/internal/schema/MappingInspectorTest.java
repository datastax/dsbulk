/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.dsbulk.engine.internal.schema.MappingInspector.INTERNAL_FUNCTION_MARKER;
import static com.datastax.dsbulk.engine.internal.schema.MappingInspector.INTERNAL_TIMESTAMP_VARNAME;
import static com.datastax.dsbulk.engine.internal.schema.MappingInspector.INTERNAL_TTL_VARNAME;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import org.junit.jupiter.api.Test;

class MappingInspectorTest {

  @Test
  void should_parse_mapped_mapping() {
    assertThat(new MappingInspector("fieldA=col1,fieldB=col2", false).getExplicitVariables())
        .containsEntry("fieldA", "col1")
        .containsEntry("fieldB", "col2");
    assertThat(new MappingInspector("fieldA:col1,fieldB:col2", false).getExplicitVariables())
        .containsEntry("fieldA", "col1")
        .containsEntry("fieldB", "col2");
    assertThat(
        new MappingInspector("  fieldA : col1 , fieldB : col2  ", false).getExplicitVariables())
        .containsEntry("fieldA", "col1")
        .containsEntry("fieldB", "col2");
  }

  @Test
  void should_parse_indexed_mapping_preferring_indexed_mappings() {
    assertThat(new MappingInspector("col1,col2", true).getExplicitVariables())
        .containsEntry("0", "col1")
        .containsEntry("1", "col2");
    assertThat(new MappingInspector("  col1  ,  col2  ", true).getExplicitVariables())
        .containsEntry("0", "col1")
        .containsEntry("1", "col2");
  }

  @Test
  void should_parse_indexed_mapping_preferring_mapped_mappings() {
    assertThat(new MappingInspector("col1,col2", false).getExplicitVariables())
        .containsEntry("col1", "col1")
        .containsEntry("col2", "col2");
    assertThat(new MappingInspector("  col1  ,  col2  ", false).getExplicitVariables())
        .containsEntry("col1", "col1")
        .containsEntry("col2", "col2");
  }

  @Test
  void should_parse_quoted_mapping() {
    assertThat(
        new MappingInspector("\"fieldA\"=\" \",\"\"\"fieldB\"\"\"=\"\"\"\"", false)
            .getExplicitVariables())
        .containsEntry("fieldA", " ")
        .containsEntry("\"fieldB\"", "\"");
    assertThat(
        new MappingInspector("\"fieldA\":\" \",\"\"\"fieldB\"\"\":\"\"\"\"", false)
            .getExplicitVariables())
        .containsEntry("fieldA", " ")
        .containsEntry("\"fieldB\"", "\"");
    assertThat(
        new MappingInspector(" \"fieldA\" = \" \" , \"\"\"fieldB\"\"\" = \"\"\"\" ", false)
            .getExplicitVariables())
        .containsEntry("fieldA", " ")
        .containsEntry("\"fieldB\"", "\"");
  }

  @Test
  void should_parse_inferred_mapping_token() {
    MappingInspector inspector = new MappingInspector(" * = * , fieldA = col1 ", false);
    assertThat(inspector.getExplicitVariables()).containsEntry("fieldA", "col1");
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables()).isEmpty();
  }

  @Test
  void should_parse_inferred_mapping_token_with_simple_exclusion() {
    MappingInspector inspector = new MappingInspector("* = -c2", false);
    assertThat(inspector.getExplicitVariables()).isEmpty();
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables()).containsOnly("c2");
    inspector = new MappingInspector("* = -\"C2\"", false);
    assertThat(inspector.getExplicitVariables()).isEmpty();
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables()).containsOnly("C2");
  }

  @Test
  void should_parse_inferred_mapping_token_with_complex_exclusion() {
    MappingInspector inspector = new MappingInspector(" * = [-c2, -c3]  ", false);
    assertThat(inspector.getExplicitVariables()).isEmpty();
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables()).containsOnly("c2", "c3");
    inspector = new MappingInspector(" * = [ - \"C2\", - \"C3\" ]  ", false);
    assertThat(inspector.getExplicitVariables()).isEmpty();
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables()).containsOnly("C2", "C3");
  }

  @Test
  void should_reorder_indexed_mapping_with_explicit_indices() {
    MappingInspector inspector = new MappingInspector(" 1 = A, 0 = B  ", false);
    assertThat(inspector.getExplicitVariables().keySet()).containsExactly("0", "1");
  }

  @Test
  void should_detect_ttl_and_timestamp_vars() {
    MappingInspector inspector = new MappingInspector(" a = __ttl, b = __timestamp  ", false);
    assertThat(inspector.getExplicitVariables().values())
        .containsExactly(INTERNAL_TTL_VARNAME, INTERNAL_TIMESTAMP_VARNAME);
  }

  // test
  @Test
  void should_detect_function() {
    MappingInspector inspector = new MappingInspector(" now() = col1, max(1, 2) = col2  ", false);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(INTERNAL_FUNCTION_MARKER + "now()", "col1")
        .containsEntry(INTERNAL_FUNCTION_MARKER + "max(1,2)", "col2");
  }

  @Test
  void should_detect_function_at_right_side_of_the_expression() {
    MappingInspector inspector =
        new MappingInspector(
            " col1 = now(), col2 = max(1, 2), ttl_a = ttl(a), wt_a = writetime(a)", false);
    assertThat(inspector.getExplicitVariables())
        .containsEntry("col1", INTERNAL_FUNCTION_MARKER + "now()")
        .containsEntry("col2", INTERNAL_FUNCTION_MARKER + "max(1,2)")
        .containsEntry("ttl_a", INTERNAL_FUNCTION_MARKER + "ttl(a)")
        .containsEntry("wt_a", INTERNAL_FUNCTION_MARKER + "writetime(a)");
    ;
  }

  @Test
  void should_error_out_if_syntax_error() {
    assertThatThrownBy(() -> new MappingInspector(" { a = b, c = d ", false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("Invalid schema.mapping: mapping could not be parsed");
    assertThatThrownBy(() -> new MappingInspector(" a = b c = d ", false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("Invalid schema.mapping: mapping could not be parsed");
  }

  @Test
  void should_error_out_inferred_token_appears_twice() {
    assertThatThrownBy(() -> new MappingInspector(" *=*, *=-c2", false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid schema.mapping: inferred mapping entry (* = *) can be supplied at most once");
  }

  @Test
  void should_error_out_same_variable_mapped_twice() {
    assertThatThrownBy(() -> new MappingInspector(" a = c1, b = c1, c = c2, d = c2", false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid schema.mapping: the following variables are mapped to more than one field: c1, c2");
  }
}
