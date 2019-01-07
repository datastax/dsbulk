/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.driver.core.ProtocolVersion.V1;
import static com.datastax.dsbulk.engine.WorkflowType.COUNT;
import static com.datastax.dsbulk.engine.WorkflowType.LOAD;
import static com.datastax.dsbulk.engine.WorkflowType.UNLOAD;
import static com.datastax.dsbulk.engine.internal.schema.MappingPreference.INDEXED_ONLY;
import static com.datastax.dsbulk.engine.internal.schema.MappingPreference.MAPPED_ONLY;
import static com.datastax.dsbulk.engine.internal.schema.MappingPreference.MAPPED_OR_INDEXED;
import static com.datastax.dsbulk.engine.internal.schema.QueryInspector.INTERNAL_TIMESTAMP_VARNAME;
import static com.datastax.dsbulk.engine.internal.schema.QueryInspector.INTERNAL_TTL_VARNAME;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.EnumSource.Mode.EXCLUDE;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.dsbulk.commons.config.BulkConfigurationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class MappingInspectorTest {

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_parse_mapped_mapping(ProtocolVersion version) {
    assertThat(
            new MappingInspector(
                    "fieldA=col1,fieldB=col2",
                    LOAD,
                    MAPPED_ONLY,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME)
                .getExplicitVariables())
        .containsEntry(new MappedMappingField("fieldA"), CQLIdentifier.fromCql("col1"))
        .containsEntry(new MappedMappingField("fieldB"), CQLIdentifier.fromCql("col2"));
    assertThat(
            new MappingInspector(
                    "fieldA:col1,fieldB:col2",
                    LOAD,
                    MAPPED_ONLY,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME)
                .getExplicitVariables())
        .containsEntry(new MappedMappingField("fieldA"), CQLIdentifier.fromCql("col1"))
        .containsEntry(new MappedMappingField("fieldB"), CQLIdentifier.fromCql("col2"));
    assertThat(
            new MappingInspector(
                    "  fieldA : col1 , fieldB : col2  ",
                    LOAD,
                    MAPPED_ONLY,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME)
                .getExplicitVariables())
        .containsEntry(new MappedMappingField("fieldA"), CQLIdentifier.fromCql("col1"))
        .containsEntry(new MappedMappingField("fieldB"), CQLIdentifier.fromCql("col2"));
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_parse_simple_mapping_as_indexed_when_indexed_mapping_only_supported(
      ProtocolVersion version) {
    assertThat(
            new MappingInspector(
                    "col1,col2",
                    LOAD,
                    INDEXED_ONLY,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME)
                .getExplicitVariables())
        .containsEntry(new IndexedMappingField(0), CQLIdentifier.fromCql("col1"))
        .containsEntry(new IndexedMappingField(1), CQLIdentifier.fromCql("col2"));
    assertThat(
            new MappingInspector(
                    "  col1  ,  col2  ",
                    LOAD,
                    INDEXED_ONLY,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME)
                .getExplicitVariables())
        .containsEntry(new IndexedMappingField(0), CQLIdentifier.fromCql("col1"))
        .containsEntry(new IndexedMappingField(1), CQLIdentifier.fromCql("col2"));
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_parse_simple_mapping_as_mapped_when_mapped_mapping_only_supported(
      ProtocolVersion version) {
    assertThat(
            new MappingInspector(
                    "col1,col2",
                    LOAD,
                    MAPPED_ONLY,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME)
                .getExplicitVariables())
        .containsEntry(new MappedMappingField("col1"), CQLIdentifier.fromCql("col1"))
        .containsEntry(new MappedMappingField("col2"), CQLIdentifier.fromCql("col2"));
    assertThat(
            new MappingInspector(
                    "  col1  ,  col2  ",
                    LOAD,
                    MAPPED_ONLY,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME)
                .getExplicitVariables())
        .containsEntry(new MappedMappingField("col1"), CQLIdentifier.fromCql("col1"))
        .containsEntry(new MappedMappingField("col2"), CQLIdentifier.fromCql("col2"));
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_parse_simple_mapping_as_mapped_when_both_mapped_and_indexed_mappings_supported(
      ProtocolVersion version) {
    assertThat(
            new MappingInspector(
                    "col1,col2",
                    LOAD,
                    MAPPED_OR_INDEXED,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME)
                .getExplicitVariables())
        .containsEntry(new MappedMappingField("col1"), CQLIdentifier.fromCql("col1"))
        .containsEntry(new MappedMappingField("col2"), CQLIdentifier.fromCql("col2"));
    assertThat(
            new MappingInspector(
                    "  col1  ,  col2  ",
                    LOAD,
                    MAPPED_OR_INDEXED,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME)
                .getExplicitVariables())
        .containsEntry(new MappedMappingField("col1"), CQLIdentifier.fromCql("col1"))
        .containsEntry(new MappedMappingField("col2"), CQLIdentifier.fromCql("col2"));
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_parse_indexed_mapping_as_indexed_when_indexed_mapping_supported(
      ProtocolVersion version) {
    assertThat(
            new MappingInspector(
                    "0=col1,1=col2",
                    LOAD,
                    INDEXED_ONLY,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME)
                .getExplicitVariables())
        .containsEntry(new IndexedMappingField(0), CQLIdentifier.fromCql("col1"))
        .containsEntry(new IndexedMappingField(1), CQLIdentifier.fromCql("col2"));
    assertThat(
            new MappingInspector(
                    "  0 = col1  , 1 = col2  ",
                    LOAD,
                    INDEXED_ONLY,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME)
                .getExplicitVariables())
        .containsEntry(new IndexedMappingField(0), CQLIdentifier.fromCql("col1"))
        .containsEntry(new IndexedMappingField(1), CQLIdentifier.fromCql("col2"));
    assertThat(
            new MappingInspector(
                    "0=col1,1=col2",
                    LOAD,
                    MAPPED_OR_INDEXED,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME)
                .getExplicitVariables())
        .containsEntry(new IndexedMappingField(0), CQLIdentifier.fromCql("col1"))
        .containsEntry(new IndexedMappingField(1), CQLIdentifier.fromCql("col2"));
    assertThat(
            new MappingInspector(
                    "  0 = col1  , 1 = col2  ",
                    LOAD,
                    MAPPED_OR_INDEXED,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME)
                .getExplicitVariables())
        .containsEntry(new IndexedMappingField(0), CQLIdentifier.fromCql("col1"))
        .containsEntry(new IndexedMappingField(1), CQLIdentifier.fromCql("col2"));
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_parse_indexed_mapping_as_mapped_when_indexed_mapping_not_supported(
      ProtocolVersion version) {
    assertThat(
            new MappingInspector(
                    "0=col1,1=col2",
                    LOAD,
                    MAPPED_ONLY,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME)
                .getExplicitVariables())
        .containsEntry(new MappedMappingField("0"), CQLIdentifier.fromCql("col1"))
        .containsEntry(new MappedMappingField("1"), CQLIdentifier.fromCql("col2"));
    assertThat(
            new MappingInspector(
                    "  0 = col1  , 1 =  col2  ",
                    LOAD,
                    MAPPED_ONLY,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME)
                .getExplicitVariables())
        .containsEntry(new MappedMappingField("0"), CQLIdentifier.fromCql("col1"))
        .containsEntry(new MappedMappingField("1"), CQLIdentifier.fromCql("col2"));
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_parse_quoted_mapping(ProtocolVersion version) {
    assertThat(
            new MappingInspector(
                    "\"fieldA\"=\" \",\"\"\"fieldB\"\"\"=\"\"\"\"",
                    LOAD,
                    MAPPED_ONLY,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME)
                .getExplicitVariables())
        .containsEntry(new MappedMappingField("fieldA"), CQLIdentifier.fromInternal(" "))
        .containsEntry(new MappedMappingField("\"fieldB\""), CQLIdentifier.fromInternal("\""));
    assertThat(
            new MappingInspector(
                    "\"fieldA\":\" \",\"\"\"fieldB\"\"\":\"\"\"\"",
                    LOAD,
                    MAPPED_ONLY,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME)
                .getExplicitVariables())
        .containsEntry(new MappedMappingField("fieldA"), CQLIdentifier.fromInternal(" "))
        .containsEntry(new MappedMappingField("\"fieldB\""), CQLIdentifier.fromInternal("\""));
    assertThat(
            new MappingInspector(
                    " \"fieldA\" = \" \" , \"\"\"fieldB\"\"\" = \"\"\"\" ",
                    LOAD,
                    MAPPED_ONLY,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME)
                .getExplicitVariables())
        .containsEntry(new MappedMappingField("fieldA"), CQLIdentifier.fromInternal(" "))
        .containsEntry(new MappedMappingField("\"fieldB\""), CQLIdentifier.fromInternal("\""));
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_parse_inferred_mapping_token(ProtocolVersion version) {
    MappingInspector inspector =
        new MappingInspector(
            " * = * , fieldA = col1 ",
            LOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(new MappedMappingField("fieldA"), CQLIdentifier.fromCql("col1"));
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables()).isEmpty();
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_parse_inferred_mapping_token_with_simple_exclusion(ProtocolVersion version) {
    MappingInspector inspector =
        new MappingInspector(
            "* = -c2",
            LOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getExplicitVariables()).isEmpty();
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables()).containsOnly(CQLIdentifier.fromInternal("c2"));
    inspector =
        new MappingInspector(
            "* = -\"C2\"",
            LOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getExplicitVariables()).isEmpty();
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables()).containsOnly(CQLIdentifier.fromInternal("C2"));
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_parse_inferred_mapping_token_with_complex_exclusion(ProtocolVersion version) {
    MappingInspector inspector =
        new MappingInspector(
            " * = [-c2, -c3]  ",
            LOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getExplicitVariables()).isEmpty();
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables())
        .containsOnly(CQLIdentifier.fromInternal("c2"), CQLIdentifier.fromInternal("c3"));
    inspector =
        new MappingInspector(
            " * = [ - \"C2\", - \"C3\" ]  ",
            LOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getExplicitVariables()).isEmpty();
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables())
        .containsOnly(CQLIdentifier.fromInternal("C2"), CQLIdentifier.fromInternal("C3"));
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_reorder_indexed_mapping_with_explicit_indices(ProtocolVersion version) {
    MappingInspector inspector =
        new MappingInspector(
            " 1 = A, 0 = B  ",
            LOAD,
            INDEXED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getExplicitVariables().keySet())
        .containsExactly(new IndexedMappingField(0), new IndexedMappingField(1));
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_detect_ttl_and_timestamp_vars(ProtocolVersion version) {
    MappingInspector inspector =
        new MappingInspector(
            " a = __ttl, b = __timestamp  ",
            LOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getExplicitVariables().values())
        .containsExactly(INTERNAL_TTL_VARNAME, INTERNAL_TIMESTAMP_VARNAME);
    assertThat(inspector.getWriteTimeVariables()).containsExactly(INTERNAL_TIMESTAMP_VARNAME);
    inspector =
        new MappingInspector(
            " a = \"[ttl]\", b = \"[timestamp]\"  ",
            LOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getExplicitVariables().values())
        .containsExactly(INTERNAL_TTL_VARNAME, INTERNAL_TIMESTAMP_VARNAME);
    assertThat(inspector.getWriteTimeVariables()).containsExactly(INTERNAL_TIMESTAMP_VARNAME);
    inspector =
        new MappingInspector(
            " a = \"MyTTL\", b = \"MyTimestamp\"  ",
            LOAD,
            MAPPED_ONLY,
            version,
            CQLIdentifier.fromInternal("MyTimestamp"),
            CQLIdentifier.fromInternal("MyTTL"));
    assertThat(inspector.getExplicitVariables().values())
        .containsExactly(
            CQLIdentifier.fromInternal("MyTTL"), CQLIdentifier.fromInternal("MyTimestamp"));
    assertThat(inspector.getWriteTimeVariables())
        .containsExactly(CQLIdentifier.fromInternal("MyTimestamp"));
  }

  @ParameterizedTest
  @EnumSource(value = ProtocolVersion.class, names = "V1", mode = EXCLUDE)
  void should_detect_writetime_function_in_variable(ProtocolVersion version) {
    MappingInspector inspector =
        new MappingInspector(
            " a = WrItEtImE(\"My Col 2\")  ",
            LOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getWriteTimeVariables())
        .containsExactly(
            new Alias(
                new FunctionCall(
                    null,
                    CQLIdentifier.fromInternal("writetime"),
                    CQLIdentifier.fromInternal("My Col 2")),
                CQLIdentifier.fromInternal("writetime(My Col 2)")));
  }

  @Test
  void should_detect_writetime_function_in_variable_v1() {
    MappingInspector inspector =
        new MappingInspector(
            " a = WrItEtImE(\"My Col 2\")  ",
            LOAD,
            MAPPED_ONLY,
            V1,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getWriteTimeVariables())
        .containsExactly(
            new FunctionCall(
                null,
                CQLIdentifier.fromInternal("writetime"),
                CQLIdentifier.fromInternal("My Col 2")));
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_detect_function_in_field(ProtocolVersion version) {
    MappingInspector inspector =
        new MappingInspector(
            " now() = col1, max(1, 2) = col2  ",
            LOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(
            new FunctionCall(null, CQLIdentifier.fromInternal("now")),
            CQLIdentifier.fromCql("col1"))
        .containsEntry(
            new FunctionCall(
                null, CQLIdentifier.fromInternal("max"), new CQLLiteral("1"), new CQLLiteral("2")),
            CQLIdentifier.fromCql("col2"));
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_detect_function_in_field_case_sensitive(ProtocolVersion version) {
    MappingInspector inspector =
        new MappingInspector(
            "\"MAX\"(1, 2) = col2  ",
            LOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(
            new FunctionCall(
                null, CQLIdentifier.fromInternal("MAX"), new CQLLiteral("1"), new CQLLiteral("2")),
            CQLIdentifier.fromCql("col2"));
  }

  @ParameterizedTest
  @EnumSource(value = ProtocolVersion.class, names = "V1", mode = EXCLUDE)
  void should_detect_function_in_variable(ProtocolVersion version) {
    MappingInspector inspector =
        new MappingInspector(
            " fieldA = now(), fieldB = max(1,2), fieldC = ttl(a), fieldD = writetime(a)",
            LOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(
            new MappedMappingField("fieldA"),
            new Alias(
                new FunctionCall(null, CQLIdentifier.fromInternal("now")),
                CQLIdentifier.fromInternal("now()")))
        .containsEntry(
            new MappedMappingField("fieldB"),
            new Alias(
                new FunctionCall(
                    null,
                    CQLIdentifier.fromInternal("max"),
                    new CQLLiteral("1"),
                    new CQLLiteral("2")),
                CQLIdentifier.fromInternal("max(1, 2)")))
        .containsEntry(
            new MappedMappingField("fieldC"),
            new Alias(
                new FunctionCall(
                    null, CQLIdentifier.fromInternal("ttl"), CQLIdentifier.fromInternal("a")),
                CQLIdentifier.fromInternal("ttl(a)")))
        .containsEntry(
            new MappedMappingField("fieldD"),
            new Alias(
                new FunctionCall(
                    null, CQLIdentifier.fromInternal("writetime"), CQLIdentifier.fromInternal("a")),
                CQLIdentifier.fromInternal("writetime(a)")));
    assertThat(inspector.getWriteTimeVariables())
        .containsExactly(
            new Alias(
                new FunctionCall(
                    null, CQLIdentifier.fromInternal("writetime"), CQLIdentifier.fromInternal("a")),
                CQLIdentifier.fromInternal("writetime(a)")));
  }

  @Test
  void should_detect_function_in_variable_v1() {
    MappingInspector inspector =
        new MappingInspector(
            " fieldA = now(), fieldB = max(1,2), fieldC = ttl(a), fieldD = writetime(a)",
            LOAD,
            MAPPED_ONLY,
            V1,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(
            new MappedMappingField("fieldA"),
            new FunctionCall(null, CQLIdentifier.fromInternal("now")))
        .containsEntry(
            new MappedMappingField("fieldB"),
            new FunctionCall(
                null, CQLIdentifier.fromInternal("max"), new CQLLiteral("1"), new CQLLiteral("2")))
        .containsEntry(
            new MappedMappingField("fieldC"),
            new FunctionCall(
                null, CQLIdentifier.fromInternal("ttl"), CQLIdentifier.fromInternal("a")))
        .containsEntry(
            new MappedMappingField("fieldD"),
            new FunctionCall(
                null, CQLIdentifier.fromInternal("writetime"), CQLIdentifier.fromInternal("a")));
    assertThat(inspector.getWriteTimeVariables())
        .containsExactly(
            new FunctionCall(
                null, CQLIdentifier.fromInternal("writetime"), CQLIdentifier.fromInternal("a")));
  }

  @ParameterizedTest
  @EnumSource(value = ProtocolVersion.class, names = "V1", mode = EXCLUDE)
  void should_detect_function_in_variable_case_sensitive(ProtocolVersion version) {
    MappingInspector inspector =
        new MappingInspector(
            "fieldA = \"MAX\"(\"My Col 1\", \"My Col 2\")",
            LOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(
            new MappedMappingField("fieldA"),
            new Alias(
                new FunctionCall(
                    null,
                    CQLIdentifier.fromInternal("MAX"),
                    CQLIdentifier.fromInternal("My Col 1"),
                    CQLIdentifier.fromInternal("My Col 2")),
                CQLIdentifier.fromInternal("MAX(My Col 1, My Col 2)")));
  }

  @Test
  void should_detect_function_in_variable_case_sensitive_v1() {
    MappingInspector inspector =
        new MappingInspector(
            "fieldA = \"MAX\"(\"My Col 1\", \"My Col 2\")",
            LOAD,
            MAPPED_ONLY,
            V1,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(
            new MappedMappingField("fieldA"),
            new FunctionCall(
                null,
                CQLIdentifier.fromInternal("MAX"),
                CQLIdentifier.fromInternal("My Col 1"),
                CQLIdentifier.fromInternal("My Col 2")));
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_reject_function_in_simple_entry_when_loading(ProtocolVersion version) {
    assertThatThrownBy(
            () ->
                new MappingInspector(
                    "a,b,c,now()",
                    LOAD,
                    MAPPED_ONLY,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("simple entries cannot contain function calls when loading");
  }

  @ParameterizedTest
  @EnumSource(value = ProtocolVersion.class, names = "V1", mode = EXCLUDE)
  void should_accept_function_in_simple_entry_when_unloading(ProtocolVersion version) {
    MappingInspector inspector =
        new MappingInspector(
            "a,b,c,now()",
            UNLOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(
            new MappedMappingField("now()"),
            new Alias(
                new FunctionCall(null, CQLIdentifier.fromInternal("now")),
                CQLIdentifier.fromInternal("now()")));
    inspector =
        new MappingInspector(
            "a,b,c,now()",
            UNLOAD,
            INDEXED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(
            new IndexedMappingField(3),
            new Alias(
                new FunctionCall(null, CQLIdentifier.fromInternal("now")),
                CQLIdentifier.fromInternal("now()")));
  }

  @Test
  void should_accept_function_in_simple_entry_when_unloading_v1() {
    MappingInspector inspector =
        new MappingInspector(
            "a,b,c,now()",
            UNLOAD,
            MAPPED_ONLY,
            V1,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(
            new MappedMappingField("now()"),
            new FunctionCall(null, CQLIdentifier.fromInternal("now")));
    inspector =
        new MappingInspector(
            "a,b,c,now()",
            UNLOAD,
            INDEXED_ONLY,
            V1,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(
            new IndexedMappingField(3), new FunctionCall(null, CQLIdentifier.fromInternal("now")));
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_error_out_if_syntax_error(ProtocolVersion version) {
    assertThatThrownBy(
            () ->
                new MappingInspector(
                    " { a = b, c = d ",
                    LOAD,
                    MAPPED_ONLY,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("Invalid schema.mapping: mapping could not be parsed");
    assertThatThrownBy(
            () ->
                new MappingInspector(
                    " a = b c = d ",
                    LOAD,
                    MAPPED_ONLY,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining("Invalid schema.mapping: mapping could not be parsed");
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_error_out_inferred_token_appears_twice(ProtocolVersion version) {
    assertThatThrownBy(
            () ->
                new MappingInspector(
                    " *=*, *=-c2",
                    LOAD,
                    MAPPED_ONLY,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid schema.mapping: inferred mapping entry (* = *) can be supplied at most once");
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_accept_same_field_mapped_twice_when_loading(ProtocolVersion version) {
    MappingInspector inspector =
        new MappingInspector(
            " a = c1, a = c2",
            LOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(new MappedMappingField("a"), CQLIdentifier.fromInternal("c1"))
        .containsEntry(new MappedMappingField("a"), CQLIdentifier.fromInternal("c2"));
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_error_out_same_variable_mapped_twice_when_loading(ProtocolVersion version) {
    assertThatThrownBy(
            () ->
                new MappingInspector(
                    " a = c1, b = c1, c = c2, d = c2",
                    LOAD,
                    MAPPED_ONLY,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid schema.mapping: the following variables are mapped to more than one field: c1, c2");
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_accept_same_variable_mapped_twice_when_unloading(ProtocolVersion version) {
    MappingInspector inspector =
        new MappingInspector(
            " a = c1, b = c1",
            UNLOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(new MappedMappingField("a"), CQLIdentifier.fromInternal("c1"))
        .containsEntry(new MappedMappingField("b"), CQLIdentifier.fromInternal("c1"));
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_error_out_same_field_mapped_twice_when_unloading(ProtocolVersion version) {
    assertThatThrownBy(
            () ->
                new MappingInspector(
                    " a = c1, a = c2, b = c3, b = c4",
                    UNLOAD,
                    MAPPED_ONLY,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid schema.mapping: the following fields are mapped to more than one variable: a, b");
    assertThatThrownBy(
            () ->
                new MappingInspector(
                    " a = c1, a = c2, b = c3, b = c4",
                    COUNT,
                    MAPPED_ONLY,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Invalid schema.mapping: the following fields are mapped to more than one variable: a, b");
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_accept_duplicate_mapping_when_loading_and_unloading(ProtocolVersion version) {
    MappingInspector inspector =
        new MappingInspector(
            " a = c1, a = c1",
            LOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getExplicitVariables())
        .hasSize(1)
        .containsEntry(new MappedMappingField("a"), CQLIdentifier.fromInternal("c1"));
    inspector =
        new MappingInspector(
            " a = c1, a = c1",
            UNLOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.getExplicitVariables())
        .hasSize(1)
        .containsEntry(new MappedMappingField("a"), CQLIdentifier.fromInternal("c1"));
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_error_out_if_mapped_mapping_but_preference_is_indexed_only(ProtocolVersion version) {
    assertThatThrownBy(
            () ->
                new MappingInspector(
                    "* = *, a = b, now() = d",
                    LOAD,
                    INDEXED_ONLY,
                    version,
                    INTERNAL_TIMESTAMP_VARNAME,
                    INTERNAL_TTL_VARNAME))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessageContaining(
            "Schema mapping contains named fields, but connector only supports indexed fields");
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_detect_using_timestamp(ProtocolVersion version) {
    MappingInspector inspector =
        new MappingInspector(
            "f1 = c1, f2 = __timestamp",
            LOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.hasUsingTimestamp()).isTrue();
    inspector =
        new MappingInspector(
            "f1 = c1, f2 = __ttl",
            LOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.hasUsingTimestamp()).isFalse();
    inspector =
        new MappingInspector(
            "f1 = c1, f2 = \"[timestamp]\"",
            LOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.hasUsingTimestamp()).isTrue();
    inspector =
        new MappingInspector(
            "f1 = c1, f2 = c2",
            LOAD,
            MAPPED_ONLY,
            version,
            CQLIdentifier.fromInternal("c2"),
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.hasUsingTimestamp()).isTrue();
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_detect_using_ttl(ProtocolVersion version) {
    MappingInspector inspector =
        new MappingInspector(
            "f1 = c1, f2 = __ttl",
            LOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.hasUsingTTL()).isTrue();
    inspector =
        new MappingInspector(
            "f1 = c1, f2 = __timestamp",
            LOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.hasUsingTTL()).isFalse();
    inspector =
        new MappingInspector(
            "f1 = c1, f2 = \"[ttl]\"",
            LOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            INTERNAL_TTL_VARNAME);
    assertThat(inspector.hasUsingTTL()).isTrue();
    inspector =
        new MappingInspector(
            "f1 = c1, f2 = c2",
            LOAD,
            MAPPED_ONLY,
            version,
            INTERNAL_TIMESTAMP_VARNAME,
            CQLIdentifier.fromInternal("c2"));
    assertThat(inspector.hasUsingTTL()).isTrue();
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_error_when_mapped_ttl_but_no_using_ttl_variable(ProtocolVersion version) {
    assertThatThrownBy(
            () ->
                new MappingInspector("f1 = c1, f2 = __ttl", LOAD, MAPPED_ONLY, version, null, null))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Invalid mapping: __ttl variable is not allowed when schema.query does not contain a USING TTL clause");
  }

  @ParameterizedTest
  @EnumSource(ProtocolVersion.class)
  void should_error_when_mapped_timestamp_but_no_using_timestamp_variable(ProtocolVersion version) {
    assertThatThrownBy(
            () ->
                new MappingInspector(
                    "f1 = c1, f2 = __timestamp", LOAD, MAPPED_ONLY, version, null, null))
        .isInstanceOf(BulkConfigurationException.class)
        .hasMessage(
            "Invalid mapping: __timestamp variable is not allowed when schema.query does not contain a USING TIMESTAMP clause");
  }

  @ParameterizedTest
  @EnumSource(value = ProtocolVersion.class, names = "V1", mode = EXCLUDE)
  void should_detect_qualified_function_name(ProtocolVersion version) {
    MappingInspector inspector =
        new MappingInspector("\"MyKs1\".\"NOW\"() = c1", LOAD, MAPPED_ONLY, version, null, null);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(
            new FunctionCall(
                CQLIdentifier.fromInternal("MyKs1"), CQLIdentifier.fromInternal("NOW")),
            CQLIdentifier.fromInternal("c1"));
    inspector =
        new MappingInspector(
            "f1 = \"MyKs1\".\"PLUS\"(\"C1\",\"C2\")", UNLOAD, MAPPED_ONLY, version, null, null);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(
            new MappedMappingField("f1"),
            new Alias(
                new FunctionCall(
                    CQLIdentifier.fromInternal("MyKs1"),
                    CQLIdentifier.fromInternal("PLUS"),
                    CQLIdentifier.fromInternal("C1"),
                    CQLIdentifier.fromInternal("C2")),
                CQLIdentifier.fromInternal("MyKs1.PLUS(C1, C2)")));
  }

  @Test
  void should_detect_qualified_function_name_v1() {
    MappingInspector inspector =
        new MappingInspector("\"MyKs1\".\"NOW\"() = c1", LOAD, MAPPED_ONLY, V1, null, null);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(
            new FunctionCall(
                CQLIdentifier.fromInternal("MyKs1"), CQLIdentifier.fromInternal("NOW")),
            CQLIdentifier.fromInternal("c1"));
    inspector =
        new MappingInspector(
            "f1 = \"MyKs1\".\"PLUS\"(\"C1\",\"C2\")", UNLOAD, MAPPED_ONLY, V1, null, null);
    assertThat(inspector.getExplicitVariables())
        .containsEntry(
            new MappedMappingField("f1"),
            new FunctionCall(
                CQLIdentifier.fromInternal("MyKs1"),
                CQLIdentifier.fromInternal("PLUS"),
                CQLIdentifier.fromInternal("C1"),
                CQLIdentifier.fromInternal("C2")));
  }
}
