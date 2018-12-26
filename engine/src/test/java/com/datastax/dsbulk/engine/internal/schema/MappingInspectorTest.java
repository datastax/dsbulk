/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.dsbulk.engine.WorkflowType.COUNT;
import static com.datastax.dsbulk.engine.WorkflowType.LOAD;
import static com.datastax.dsbulk.engine.WorkflowType.UNLOAD;
import static com.datastax.dsbulk.engine.internal.schema.MappingInspector.INTERNAL_TIMESTAMP_VARNAME;
import static com.datastax.dsbulk.engine.internal.schema.MappingInspector.INTERNAL_TTL_VARNAME;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import org.junit.jupiter.api.Test;

class MappingInspectorTest {

  @Test
  void should_parse_mapped_mapping() {
    assertThat(new MappingInspector("fieldA=col1,fieldB=col2", false, LOAD).getExplicitVariables())
        .containsEntry(new MappedMappingField("fieldA"), CQLIdentifier.fromCql("col1"))
        .containsEntry(new MappedMappingField("fieldB"), CQLIdentifier.fromCql("col2"));
    assertThat(new MappingInspector("fieldA:col1,fieldB:col2", false, LOAD).getExplicitVariables())
        .containsEntry(new MappedMappingField("fieldA"), CQLIdentifier.fromCql("col1"))
        .containsEntry(new MappedMappingField("fieldB"), CQLIdentifier.fromCql("col2"));
    assertThat(
            new MappingInspector("  fieldA : col1 , fieldB : col2  ", false, LOAD)
                .getExplicitVariables())
        .containsEntry(new MappedMappingField("fieldA"), CQLIdentifier.fromCql("col1"))
        .containsEntry(new MappedMappingField("fieldB"), CQLIdentifier.fromCql("col2"));
  }

  @Test
  void should_parse_indexed_mapping_preferring_indexed_mappings() {
    assertThat(new MappingInspector("col1,col2", true, LOAD).getExplicitVariables())
        .containsEntry(new IndexedMappingField(0), CQLIdentifier.fromCql("col1"))
        .containsEntry(new IndexedMappingField(1), CQLIdentifier.fromCql("col2"));
    assertThat(new MappingInspector("  col1  ,  col2  ", true, LOAD).getExplicitVariables())
        .containsEntry(new IndexedMappingField(0), CQLIdentifier.fromCql("col1"))
        .containsEntry(new IndexedMappingField(1), CQLIdentifier.fromCql("col2"));
  }

  @Test
  void should_parse_indexed_mapping_preferring_mapped_mappings() {
    assertThat(new MappingInspector("col1,col2", false, LOAD).getExplicitVariables())
        .containsEntry(new MappedMappingField("col1"), CQLIdentifier.fromCql("col1"))
        .containsEntry(new MappedMappingField("col2"), CQLIdentifier.fromCql("col2"));
    assertThat(new MappingInspector("  col1  ,  col2  ", false, LOAD).getExplicitVariables())
        .containsEntry(new MappedMappingField("col1"), CQLIdentifier.fromCql("col1"))
        .containsEntry(new MappedMappingField("col2"), CQLIdentifier.fromCql("col2"));
  }

  @Test
  void should_parse_quoted_mapping() {
    assertThat(
            new MappingInspector("\"fieldA\"=\" \",\"\"\"fieldB\"\"\"=\"\"\"\"", false, LOAD)
                .getExplicitVariables())
        .containsEntry(new MappedMappingField("fieldA"), CQLIdentifier.fromInternal(" "))
        .containsEntry(new MappedMappingField("\"fieldB\""), CQLIdentifier.fromInternal("\""));
    assertThat(
            new MappingInspector("\"fieldA\":\" \",\"\"\"fieldB\"\"\":\"\"\"\"", false, LOAD)
                .getExplicitVariables())
        .containsEntry(new MappedMappingField("fieldA"), CQLIdentifier.fromInternal(" "))
        .containsEntry(new MappedMappingField("\"fieldB\""), CQLIdentifier.fromInternal("\""));
    assertThat(
            new MappingInspector(
                    " \"fieldA\" = \" \" , \"\"\"fieldB\"\"\" = \"\"\"\" ", false, LOAD)
                .getExplicitVariables())
        .containsEntry(new MappedMappingField("fieldA"), CQLIdentifier.fromInternal(" "))
        .containsEntry(new MappedMappingField("\"fieldB\""), CQLIdentifier.fromInternal("\""));
  }

  @Test
  void should_parse_inferred_mapping_token() {
    MappingInspector inspector = new MappingInspector(" * = * , fieldA = col1 ", false, LOAD);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(new MappedMappingField("fieldA"), CQLIdentifier.fromCql("col1"));
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables()).isEmpty();
  }

  @Test
  void should_parse_inferred_mapping_token_with_simple_exclusion() {
    MappingInspector inspector = new MappingInspector("* = -c2", false, LOAD);
    assertThat(inspector.getExplicitVariables()).isEmpty();
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables()).containsOnly(CQLIdentifier.fromInternal("c2"));
    inspector = new MappingInspector("* = -\"C2\"", false, LOAD);
    assertThat(inspector.getExplicitVariables()).isEmpty();
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables()).containsOnly(CQLIdentifier.fromInternal("C2"));
  }

  @Test
  void should_parse_inferred_mapping_token_with_complex_exclusion() {
    MappingInspector inspector = new MappingInspector(" * = [-c2, -c3]  ", false, LOAD);
    assertThat(inspector.getExplicitVariables()).isEmpty();
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables())
        .containsOnly(CQLIdentifier.fromInternal("c2"), CQLIdentifier.fromInternal("c3"));
    inspector = new MappingInspector(" * = [ - \"C2\", - \"C3\" ]  ", false, LOAD);
    assertThat(inspector.getExplicitVariables()).isEmpty();
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables())
        .containsOnly(CQLIdentifier.fromInternal("C2"), CQLIdentifier.fromInternal("C3"));
  }

  @Test
  void should_reorder_indexed_mapping_with_explicit_indices() {
    MappingInspector inspector = new MappingInspector(" 1 = A, 0 = B  ", false, LOAD);
    assertThat(inspector.getExplicitVariables().keySet())
        .containsExactly(new IndexedMappingField(0), new IndexedMappingField(1));
  }

  @Test
  void should_detect_ttl_and_timestamp_vars() {
    MappingInspector inspector = new MappingInspector(" a = __ttl, b = __timestamp  ", false, LOAD);
    assertThat(inspector.getExplicitVariables().values())
        .containsExactly(INTERNAL_TTL_VARNAME, INTERNAL_TIMESTAMP_VARNAME);
  }

  @Test
  void should_detect_writetime_function_in_variable() {
    MappingInspector inspector =
        new MappingInspector(" a = WrItEtImE(\"My Col 2\")  ", false, LOAD);
    assertThat(inspector.getWriteTimeVariables())
        .containsExactly(
            new FunctionCall(
                CQLIdentifier.fromInternal("writetime"), CQLIdentifier.fromInternal("My Col 2")));
  }

  @Test
  void should_detect_function_in_field() {
    MappingInspector inspector =
        new MappingInspector(" now() = col1, max(1, 2) = col2  ", false, LOAD);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(
            new FunctionCall(CQLIdentifier.fromInternal("now")), CQLIdentifier.fromCql("col1"))
        .containsEntry(
            new FunctionCall(
                CQLIdentifier.fromInternal("max"), new CQLLiteral("1"), new CQLLiteral("2")),
            CQLIdentifier.fromCql("col2"));
  }

  @Test
  void should_detect_function_in_field_case_sensitive() {
    MappingInspector inspector =
        new MappingInspector("\"MAX\"(\"My Col 1\", \"My Col 2\") = col2  ", false, LOAD);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(
            new FunctionCall(
                CQLIdentifier.fromInternal("MAX"),
                CQLIdentifier.fromInternal("My Col 1"),
                CQLIdentifier.fromInternal("My Col 2")),
            CQLIdentifier.fromCql("col2"));
  }

  @Test
  void should_detect_function_in_variable() {
    MappingInspector inspector =
        new MappingInspector(
            " fieldA = now(), fieldB = max(1,2), fieldC = ttl(a), fieldD = writetime(a)",
            false,
            LOAD);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(
            new MappedMappingField("fieldA"), new FunctionCall(CQLIdentifier.fromInternal("now")))
        .containsEntry(
            new MappedMappingField("fieldB"),
            new FunctionCall(
                CQLIdentifier.fromInternal("max"), new CQLLiteral("1"), new CQLLiteral("2")))
        .containsEntry(
            new MappedMappingField("fieldC"),
            new FunctionCall(CQLIdentifier.fromInternal("ttl"), CQLIdentifier.fromInternal("a")))
        .containsEntry(
            new MappedMappingField("fieldD"),
            new FunctionCall(
                CQLIdentifier.fromInternal("writetime"), CQLIdentifier.fromInternal("a")));
  }

  @Test
  void should_detect_function_in_variable_case_sensitive() {
    MappingInspector inspector =
        new MappingInspector("fieldA = \"MAX\"(\"My Col 1\", \"My Col 2\")", false, LOAD);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(
            new MappedMappingField("fieldA"),
            new FunctionCall(
                CQLIdentifier.fromInternal("MAX"),
                CQLIdentifier.fromInternal("My Col 1"),
                CQLIdentifier.fromInternal("My Col 2")));
  }

  @Test
  void should_reject_function_in_simple_entry_when_loading() {
    assertThatThrownBy(() -> new MappingInspector("a,b,c,now()", false, LOAD))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("simple entries cannot contain function calls when loading");
  }

  @Test
  void should_accept_function_in_simple_entry_when_unloading() {
    MappingInspector inspector = new MappingInspector("a,b,c,now()", false, UNLOAD);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(
            new MappedMappingField("now()"), new FunctionCall(CQLIdentifier.fromInternal("now")));
    inspector = new MappingInspector("a,b,c,now()", true, UNLOAD);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(
            new IndexedMappingField(3), new FunctionCall(CQLIdentifier.fromInternal("now")));
  }

  @Test
  void should_error_out_if_syntax_error() {
    assertThatThrownBy(() -> new MappingInspector(" { a = b, c = d ", false, LOAD))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("Invalid schema.mapping: mapping could not be parsed");
    assertThatThrownBy(() -> new MappingInspector(" a = b c = d ", false, LOAD))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("Invalid schema.mapping: mapping could not be parsed");
  }

  @Test
  void should_error_out_inferred_token_appears_twice() {
    assertThatThrownBy(() -> new MappingInspector(" *=*, *=-c2", false, LOAD))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid schema.mapping: inferred mapping entry (* = *) can be supplied at most once");
  }

  @Test
  void should_accept_same_field_mapped_twice_when_loading() {
    MappingInspector inspector = new MappingInspector(" a = c1, a = c2", false, LOAD);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(new MappedMappingField("a"), CQLIdentifier.fromInternal("c1"))
        .containsEntry(new MappedMappingField("a"), CQLIdentifier.fromInternal("c2"));
  }

  @Test
  void should_error_out_same_variable_mapped_twice_when_loading() {
    assertThatThrownBy(() -> new MappingInspector(" a = c1, b = c1, c = c2, d = c2", false, LOAD))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid schema.mapping: the following variables are mapped to more than one field: c1, c2");
  }

  @Test
  void should_accept_same_variable_mapped_twice_when_unloading() {
    MappingInspector inspector = new MappingInspector(" a = c1, b = c1", false, UNLOAD);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(new MappedMappingField("a"), CQLIdentifier.fromInternal("c1"))
        .containsEntry(new MappedMappingField("b"), CQLIdentifier.fromInternal("c1"));
  }

  @Test
  void should_error_out_same_field_mapped_twice_when_unloading() {
    assertThatThrownBy(() -> new MappingInspector(" a = c1, a = c2, b = c3, b = c4", false, UNLOAD))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid schema.mapping: the following fields are mapped to more than one variable: a, b");
    assertThatThrownBy(() -> new MappingInspector(" a = c1, a = c2, b = c3, b = c4", false, COUNT))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid schema.mapping: the following fields are mapped to more than one variable: a, b");
  }

  @Test
  void should_report_indexed_mapping() {
    // indexed entries: always indexed
    assertThat(new MappingInspector("now()=a,1=b,2=c", false, LOAD).isIndexed()).isTrue();
    assertThat(new MappingInspector("now()=a,1=b,2=c", true, LOAD).isIndexed()).isTrue();
    // mapped entries: never indexed
    assertThat(new MappingInspector("now()=foo,bar=qix", false, LOAD).isIndexed()).isFalse();
    assertThat(new MappingInspector("now()=foo,bar=qix", true, LOAD).isIndexed()).isFalse();
    // simple entries: indexed only when preferIndexedMapping is true
    assertThat(new MappingInspector("a,b,c", true, LOAD).isIndexed()).isTrue();
    assertThat(new MappingInspector("a,b,c", false, LOAD).isIndexed()).isFalse();
  }
}
