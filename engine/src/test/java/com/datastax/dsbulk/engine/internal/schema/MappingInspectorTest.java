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

class MappingInspectorTest {
  private static final CqlIdentifier C1 = CqlIdentifier.fromInternal("col1");
  private static final CqlIdentifier C2 = CqlIdentifier.fromInternal("col2");
  private static final CqlIdentifier C3 = CqlIdentifier.fromInternal("col3");

  @Test
  void should_parse_mapped_mapping() {
    assertThat(new MappingInspector("fieldA=col1,fieldB=col2", false).getExplicitVariables())
        .containsEntry("fieldA", C1)
        .containsEntry("fieldB", C2);
    assertThat(new MappingInspector("fieldA:col1,fieldB:col2", false).getExplicitVariables())
        .containsEntry("fieldA", C1)
        .containsEntry("fieldB", C2);
    assertThat(
            new MappingInspector("  fieldA : col1 , fieldB : col2  ", false).getExplicitVariables())
        .containsEntry("fieldA", C1)
        .containsEntry("fieldB", C2);
  }

  @Test
  void should_parse_indexed_mapping_preferring_indexed_mappings() {
    assertThat(new MappingInspector("col1,col2", true).getExplicitVariables())
        .containsEntry("0", C1)
        .containsEntry("1", C2);
    assertThat(new MappingInspector("  col1  ,  col2  ", true).getExplicitVariables())
        .containsEntry("0", C1)
        .containsEntry("1", C2);
  }

  @Test
  void should_parse_indexed_mapping_preferring_mapped_mappings() {
    assertThat(new MappingInspector("col1,col2", false).getExplicitVariables())
        .containsEntry("col1", C1)
        .containsEntry("col2", C2);
    assertThat(new MappingInspector("  col1  ,  col2  ", false).getExplicitVariables())
        .containsEntry("col1", C1)
        .containsEntry("col2", C2);
  }

  @Test
  void should_parse_quoted_mapping() {
    CqlIdentifier spaceId = CqlIdentifier.fromInternal(" ");
    CqlIdentifier quoteId = CqlIdentifier.fromInternal("\"");
    assertThat(
            new MappingInspector("\"fieldA\"=\" \",\"\"\"fieldB\"\"\"=\"\"\"\"", false)
                .getExplicitVariables())
        .containsEntry("fieldA", spaceId)
        .containsEntry("\"fieldB\"", quoteId);
    assertThat(
            new MappingInspector("\"fieldA\":\" \",\"\"\"fieldB\"\"\":\"\"\"\"", false)
                .getExplicitVariables())
        .containsEntry("fieldA", spaceId)
        .containsEntry("\"fieldB\"", quoteId);
    assertThat(
            new MappingInspector(" \"fieldA\" = \" \" , \"\"\"fieldB\"\"\" = \"\"\"\" ", false)
                .getExplicitVariables())
        .containsEntry("fieldA", spaceId)
        .containsEntry("\"fieldB\"", quoteId);
  }

  @Test
  void should_parse_inferred_mapping_token() {
    MappingInspector inspector = new MappingInspector(" * = * , fieldA = col1 ", false);
    assertThat(inspector.getExplicitVariables()).containsEntry("fieldA", C1);
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables()).isEmpty();
  }

  @Test
  void should_parse_inferred_mapping_token_with_simple_exclusion() {
    MappingInspector inspector = new MappingInspector("* = -col2", false);
    assertThat(inspector.getExplicitVariables()).isEmpty();
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables()).containsOnly(C2);
    inspector = new MappingInspector("* = -\"C2\"", false);
    assertThat(inspector.getExplicitVariables()).isEmpty();
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables()).containsOnly(CqlIdentifier.fromInternal("C2"));
  }

  @Test
  void should_parse_inferred_mapping_token_with_complex_exclusion() {
    MappingInspector inspector = new MappingInspector(" * = [-col2, -col3]  ", false);
    assertThat(inspector.getExplicitVariables()).isEmpty();
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables()).containsOnly(C2, C3);
    inspector = new MappingInspector(" * = [ - \"C2\", - \"C3\" ]  ", false);
    assertThat(inspector.getExplicitVariables()).isEmpty();
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables())
        .containsOnly(CqlIdentifier.fromInternal("C2"), CqlIdentifier.fromInternal("C3"));
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
        .containsExactly(
            CqlIdentifier.fromInternal(MappingInspector.INTERNAL_TTL_VARNAME),
            CqlIdentifier.fromInternal(MappingInspector.INTERNAL_TIMESTAMP_VARNAME));
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
    assertThatThrownBy(() -> new MappingInspector(" *=*, *=-col2", false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid schema.mapping: inferred mapping entry (* = *) can be supplied at most once");
  }

  @Test
  void should_error_out_same_variable_mapped_twice() {
    assertThatThrownBy(() -> new MappingInspector(" a = col1, b = col1, c = col2, d = col2", false))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid schema.mapping: the following variables are mapped to more than one field: col1, col2");
  }
}
