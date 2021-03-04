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
package com.datastax.oss.dsbulk.mapping;

import static com.datastax.oss.dsbulk.mapping.MappingInspector.STAR;
import static com.datastax.oss.dsbulk.mapping.MappingInspector.TTL;
import static com.datastax.oss.dsbulk.mapping.MappingInspector.WRITETIME;
import static com.datastax.oss.dsbulk.mapping.MappingPreference.INDEXED_ONLY;
import static com.datastax.oss.dsbulk.mapping.MappingPreference.MAPPED_ONLY;
import static com.datastax.oss.dsbulk.mapping.MappingPreference.MAPPED_OR_INDEXED;
import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThatThrownBy;

import com.datastax.oss.dsbulk.tests.logging.LogInterceptingExtension;
import com.datastax.oss.dsbulk.tests.logging.LogInterceptor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(LogInterceptingExtension.class)
class MappingInspectorTest {

  @Test
  void should_parse_mapped_mapping() {
    assertThat(
            new MappingInspector("fieldA=col1,fieldB=col2", true, MAPPED_ONLY)
                .getExplicitMappings())
        .containsEntry(new MappedMappingField("fieldA"), CQLWord.fromCql("col1"))
        .containsEntry(new MappedMappingField("fieldB"), CQLWord.fromCql("col2"));
    assertThat(
            new MappingInspector("fieldA:col1,fieldB:col2", true, MAPPED_ONLY)
                .getExplicitMappings())
        .containsEntry(new MappedMappingField("fieldA"), CQLWord.fromCql("col1"))
        .containsEntry(new MappedMappingField("fieldB"), CQLWord.fromCql("col2"));
    assertThat(
            new MappingInspector("  fieldA : col1 , fieldB : col2  ", true, MAPPED_ONLY)
                .getExplicitMappings())
        .containsEntry(new MappedMappingField("fieldA"), CQLWord.fromCql("col1"))
        .containsEntry(new MappedMappingField("fieldB"), CQLWord.fromCql("col2"));
  }

  @Test
  void should_parse_simple_mapping_as_indexed_when_indexed_mapping_only_supported() {
    assertThat(new MappingInspector("col1,col2", true, INDEXED_ONLY).getExplicitMappings())
        .containsEntry(new IndexedMappingField(0), CQLWord.fromCql("col1"))
        .containsEntry(new IndexedMappingField(1), CQLWord.fromCql("col2"));
    assertThat(new MappingInspector("  col1  ,  col2  ", true, INDEXED_ONLY).getExplicitMappings())
        .containsEntry(new IndexedMappingField(0), CQLWord.fromCql("col1"))
        .containsEntry(new IndexedMappingField(1), CQLWord.fromCql("col2"));
  }

  @Test
  void should_parse_simple_mapping_as_mapped_when_mapped_mapping_only_supported() {
    assertThat(new MappingInspector("col1,col2", true, MAPPED_ONLY).getExplicitMappings())
        .containsEntry(new MappedMappingField("col1"), CQLWord.fromCql("col1"))
        .containsEntry(new MappedMappingField("col2"), CQLWord.fromCql("col2"));
    assertThat(new MappingInspector("  col1  ,  col2  ", true, MAPPED_ONLY).getExplicitMappings())
        .containsEntry(new MappedMappingField("col1"), CQLWord.fromCql("col1"))
        .containsEntry(new MappedMappingField("col2"), CQLWord.fromCql("col2"));
  }

  @Test
  void should_parse_simple_mapping_as_mapped_when_both_mapped_and_indexed_mappings_supported() {
    assertThat(new MappingInspector("col1,col2", true, MAPPED_OR_INDEXED).getExplicitMappings())
        .containsEntry(new MappedMappingField("col1"), CQLWord.fromCql("col1"))
        .containsEntry(new MappedMappingField("col2"), CQLWord.fromCql("col2"));
    assertThat(
            new MappingInspector("  col1  ,  col2  ", true, MAPPED_OR_INDEXED)
                .getExplicitMappings())
        .containsEntry(new MappedMappingField("col1"), CQLWord.fromCql("col1"))
        .containsEntry(new MappedMappingField("col2"), CQLWord.fromCql("col2"));
  }

  @Test
  void should_parse_indexed_mapping_as_indexed_when_indexed_mapping_supported() {
    assertThat(new MappingInspector("0=col1,1=col2", true, INDEXED_ONLY).getExplicitMappings())
        .containsEntry(new IndexedMappingField(0), CQLWord.fromCql("col1"))
        .containsEntry(new IndexedMappingField(1), CQLWord.fromCql("col2"));
    assertThat(
            new MappingInspector("  0 = col1  , 1 = col2  ", true, INDEXED_ONLY)
                .getExplicitMappings())
        .containsEntry(new IndexedMappingField(0), CQLWord.fromCql("col1"))
        .containsEntry(new IndexedMappingField(1), CQLWord.fromCql("col2"));
    assertThat(new MappingInspector("0=col1,1=col2", true, MAPPED_OR_INDEXED).getExplicitMappings())
        .containsEntry(new IndexedMappingField(0), CQLWord.fromCql("col1"))
        .containsEntry(new IndexedMappingField(1), CQLWord.fromCql("col2"));
    assertThat(
            new MappingInspector("  0 = col1  , 1 = col2  ", true, MAPPED_OR_INDEXED)
                .getExplicitMappings())
        .containsEntry(new IndexedMappingField(0), CQLWord.fromCql("col1"))
        .containsEntry(new IndexedMappingField(1), CQLWord.fromCql("col2"));
  }

  @Test
  void should_parse_indexed_mapping_as_mapped_when_indexed_mapping_not_supported() {
    assertThat(new MappingInspector("0=col1,1=col2", true, MAPPED_ONLY).getExplicitMappings())
        .containsEntry(new MappedMappingField("0"), CQLWord.fromCql("col1"))
        .containsEntry(new MappedMappingField("1"), CQLWord.fromCql("col2"));
    assertThat(
            new MappingInspector("  0 = col1  , 1 =  col2  ", true, MAPPED_ONLY)
                .getExplicitMappings())
        .containsEntry(new MappedMappingField("0"), CQLWord.fromCql("col1"))
        .containsEntry(new MappedMappingField("1"), CQLWord.fromCql("col2"));
  }

  @Test
  void should_parse_quoted_mapping() {
    assertThat(
            new MappingInspector("\"fieldA\"=\" \",\"\"\"fieldB\"\"\"=\"\"\"\"", true, MAPPED_ONLY)
                .getExplicitMappings())
        .containsEntry(new MappedMappingField("fieldA"), CQLWord.fromInternal(" "))
        .containsEntry(new MappedMappingField("\"fieldB\""), CQLWord.fromInternal("\""));
    assertThat(
            new MappingInspector("\"fieldA\":\" \",\"\"\"fieldB\"\"\":\"\"\"\"", true, MAPPED_ONLY)
                .getExplicitMappings())
        .containsEntry(new MappedMappingField("fieldA"), CQLWord.fromInternal(" "))
        .containsEntry(new MappedMappingField("\"fieldB\""), CQLWord.fromInternal("\""));
    assertThat(
            new MappingInspector(
                    " \"fieldA\" = \" \" , \"\"\"fieldB\"\"\" = \"\"\"\" ", true, MAPPED_ONLY)
                .getExplicitMappings())
        .containsEntry(new MappedMappingField("fieldA"), CQLWord.fromInternal(" "))
        .containsEntry(new MappedMappingField("\"fieldB\""), CQLWord.fromInternal("\""));
  }

  @Test
  void should_parse_inferred_mapping_token() {
    MappingInspector inspector = new MappingInspector(" * = * , fieldA = col1 ", true, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings())
        .containsEntry(new MappedMappingField("fieldA"), CQLWord.fromCql("col1"));
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables()).isEmpty();
  }

  @Test
  void should_parse_inferred_mapping_token_with_simple_exclusion() {
    MappingInspector inspector = new MappingInspector("* = -c2", true, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings()).isEmpty();
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables()).containsOnly(CQLWord.fromInternal("c2"));
    inspector = new MappingInspector("* = -\"C2\"", true, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings()).isEmpty();
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables()).containsOnly(CQLWord.fromInternal("C2"));
  }

  @Test
  void should_parse_inferred_mapping_token_with_complex_exclusion() {
    MappingInspector inspector = new MappingInspector(" * = [-c2, -c3]  ", true, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings()).isEmpty();
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables())
        .containsOnly(CQLWord.fromInternal("c2"), CQLWord.fromInternal("c3"));
    inspector = new MappingInspector(" * = [ - \"C2\", - \"C3\" ]  ", true, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings()).isEmpty();
    assertThat(inspector.isInferring()).isTrue();
    assertThat(inspector.getExcludedVariables())
        .containsOnly(CQLWord.fromInternal("C2"), CQLWord.fromInternal("C3"));
  }

  @Test
  void should_reorder_indexed_mapping_with_explicit_indices() {
    MappingInspector inspector =
        new MappingInspector("now() = C, 1 = A, 0 = B  ", true, INDEXED_ONLY);
    assertThat(inspector.getExplicitMappings().keySet())
        .containsExactly(
            new IndexedMappingField(0),
            new IndexedMappingField(1),
            new FunctionCall(null, CQLWord.fromInternal("now")));
  }

  @Test
  @SuppressWarnings("deprecation")
  void should_detect_ttl_and_timestamp_vars() {
    MappingInspector inspector =
        new MappingInspector(
            " a = __ttl, b = __timestamp  ",
            true,
            MAPPED_ONLY,
            () -> CQLWord.fromInternal("MyTimestamp"),
            () -> CQLWord.fromInternal("MyTTL"));
    assertThat(inspector.getExplicitMappings().values())
        .containsExactly(CQLWord.fromInternal("MyTTL"), CQLWord.fromInternal("MyTimestamp"));
  }

  @Test
  void should_warn_when_deprecated_ttl_and_timestamp_vars(LogInterceptor logs) {
    MappingInspector inspector =
        new MappingInspector(" a = __ttl, b = __timestamp  ", true, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings().values())
        .containsExactly(
            new FunctionCall(null, CQLWord.fromInternal("ttl"), STAR),
            new FunctionCall(null, CQLWord.fromInternal("writetime"), STAR));
    assertThat(logs)
        .hasMessageContaining("The special __ttl mapping token has been deprecated")
        .hasMessageContaining("The special __timestamp mapping token has been deprecated");
  }

  @Test
  void should_detect_function_in_field() {
    MappingInspector inspector =
        new MappingInspector(
            " now() = col1, max(col1, col2) = col2, plus(col1, 42) = col3  ", true, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings())
        .containsEntry(new FunctionCall(null, CQLWord.fromInternal("now")), CQLWord.fromCql("col1"))
        .containsEntry(
            new FunctionCall(
                null,
                CQLWord.fromInternal("max"),
                CQLWord.fromCql("col1"),
                CQLWord.fromCql("col2")),
            CQLWord.fromCql("col2"))
        .containsEntry(
            new FunctionCall(
                null, CQLWord.fromInternal("plus"), CQLWord.fromCql("col1"), new CQLLiteral("42")),
            CQLWord.fromCql("col3"));
  }

  @Test
  void should_detect_function_in_field_case_sensitive() {
    MappingInspector inspector =
        new MappingInspector("\"MAX\"(col1, col2) = col2  ", true, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings())
        .containsEntry(
            new FunctionCall(
                null,
                CQLWord.fromInternal("MAX"),
                CQLWord.fromCql("col1"),
                CQLWord.fromCql("col2")),
            CQLWord.fromCql("col2"));
  }

  @Test
  void should_detect_function_in_variable() {
    MappingInspector inspector =
        new MappingInspector(
            " fieldA = now(), fieldB = max(col1, col2), fieldC = ttl(a), fieldD = writetime(a)",
            true,
            MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings())
        .containsEntry(
            new MappedMappingField("fieldA"), new FunctionCall(null, CQLWord.fromInternal("now")))
        .containsEntry(
            new MappedMappingField("fieldB"),
            new FunctionCall(
                null,
                CQLWord.fromInternal("max"),
                CQLWord.fromCql("col1"),
                CQLWord.fromCql("col2")))
        .containsEntry(
            new MappedMappingField("fieldC"),
            new FunctionCall(null, CQLWord.fromInternal("ttl"), CQLWord.fromInternal("a")))
        .containsEntry(
            new MappedMappingField("fieldD"),
            new FunctionCall(null, CQLWord.fromInternal("writetime"), CQLWord.fromInternal("a")));
  }

  @Test
  void should_detect_function_in_variable_case_sensitive() {
    MappingInspector inspector =
        new MappingInspector("fieldA = \"MAX\"(\"My Col 1\", \"My Col 2\")", true, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings())
        .containsEntry(
            new MappedMappingField("fieldA"),
            new FunctionCall(
                null,
                CQLWord.fromInternal("MAX"),
                CQLWord.fromInternal("My Col 1"),
                CQLWord.fromInternal("My Col 2")));
  }

  @Test
  void should_reject_function_in_simple_entry_when_loading() {
    assertThatThrownBy(() -> new MappingInspector("a,b,c,now()", true, MAPPED_ONLY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("simple entries cannot contain function calls when loading");
  }

  @Test
  void should_accept_function_in_simple_entry_when_unloading() {
    MappingInspector inspector = new MappingInspector("a,b,c,now()", false, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings())
        .containsEntry(
            new MappedMappingField("now()"), new FunctionCall(null, CQLWord.fromInternal("now")));
    inspector = new MappingInspector("a,b,c,now()", false, INDEXED_ONLY);
    assertThat(inspector.getExplicitMappings())
        .containsEntry(
            new IndexedMappingField(3), new FunctionCall(null, CQLWord.fromInternal("now")));
  }

  @Test
  void should_detect_writetime_star_variable_when_loading() {
    MappingInspector inspector = new MappingInspector("fieldA = writetime(*)", true, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings())
        .containsEntry(new MappedMappingField("fieldA"), new FunctionCall(null, WRITETIME, STAR));
  }

  @Test
  void should_reject_writetime_star_variable_when_unloading() {
    assertThatThrownBy(() -> new MappingInspector("fieldA = writetime(*)", false, MAPPED_ONLY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("writetime(*) function calls not allowed when unloading.");
  }

  @Test
  void should_detect_writetime_args_variable_when_loading() {
    MappingInspector inspector =
        new MappingInspector("fieldA = writetime(c1,c2,c3)", true, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings())
        .containsEntry(
            new MappedMappingField("fieldA"),
            new FunctionCall(
                null,
                WRITETIME,
                CQLWord.fromInternal("c1"),
                CQLWord.fromInternal("c2"),
                CQLWord.fromInternal("c3")));
  }

  @Test
  void should_detect_writetime_args_variable_when_loading_case_sensitive() {
    MappingInspector inspector =
        new MappingInspector(" a = WrItEtImE(\"My Col 2\")  ", true, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings().values())
        .containsExactly(
            new FunctionCall(
                null, CQLWord.fromInternal("writetime"), CQLWord.fromInternal("My Col 2")));
  }

  @Test
  void should_reject_writetime_args_variable_when_unloading() {
    assertThatThrownBy(() -> new MappingInspector("fieldA = writetime(c1,c2)", false, MAPPED_ONLY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "writetime() function calls must have exactly one argument when unloading.");
  }

  @Test
  void should_reject_writetime_star_field() {
    assertThatThrownBy(() -> new MappingInspector("writetime(*) = c1", false, MAPPED_ONLY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "writetime() function calls not allowed on the left side of a mapping entry.");
    assertThatThrownBy(() -> new MappingInspector("writetime(fieldA) = c1", true, MAPPED_ONLY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "writetime() function calls not allowed on the left side of a mapping entry.");
  }

  @Test
  void should_detect_ttl_star_variable_when_loading() {
    MappingInspector inspector = new MappingInspector("fieldA = ttl(*)", true, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings())
        .containsEntry(new MappedMappingField("fieldA"), new FunctionCall(null, TTL, STAR));
  }

  @Test
  void should_reject_ttl_star_variable_when_unloading() {
    assertThatThrownBy(() -> new MappingInspector("fieldA = ttl(*)", false, MAPPED_ONLY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("ttl(*) function calls not allowed when unloading.");
  }

  @Test
  void should_detect_ttl_args_variable_when_loading() {
    MappingInspector inspector = new MappingInspector("fieldA = ttl(c1,c2,c3)", true, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings())
        .containsEntry(
            new MappedMappingField("fieldA"),
            new FunctionCall(
                null,
                TTL,
                CQLWord.fromInternal("c1"),
                CQLWord.fromInternal("c2"),
                CQLWord.fromInternal("c3")));
  }

  @Test
  void should_reject_ttl_args_variable_when_unloading() {
    assertThatThrownBy(() -> new MappingInspector("fieldA = ttl(c1,c2)", false, MAPPED_ONLY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "ttl() function calls must have exactly one argument when unloading.");
  }

  @Test
  void should_reject_ttl_star_field() {
    assertThatThrownBy(() -> new MappingInspector("ttl(*) = c1", false, MAPPED_ONLY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "ttl() function calls not allowed on the left side of a mapping entry.");
    assertThatThrownBy(() -> new MappingInspector("ttl(fieldA) = c1", true, MAPPED_ONLY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "ttl() function calls not allowed on the left side of a mapping entry.");
  }

  @Test
  void should_error_out_when_syntax_error() {
    assertThatThrownBy(() -> new MappingInspector(" { a = b, c = d ", true, MAPPED_ONLY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("mapping could not be parsed");
    assertThatThrownBy(() -> new MappingInspector(" a = b c = d ", true, MAPPED_ONLY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("mapping could not be parsed");
  }

  @Test
  void should_error_out_when_inferred_token_appears_twice() {
    assertThatThrownBy(() -> new MappingInspector(" *=*, *=-c2", true, MAPPED_ONLY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("inferred mapping entry (* = *) can be supplied at most once");
  }

  @Test
  void should_accept_same_field_mapped_twice_when_loading() {
    MappingInspector inspector = new MappingInspector(" a = c1, a = c2", true, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings())
        .containsEntry(new MappedMappingField("a"), CQLWord.fromInternal("c1"))
        .containsEntry(new MappedMappingField("a"), CQLWord.fromInternal("c2"));
  }

  @Test
  void should_error_out_same_variable_mapped_twice_when_loading() {
    assertThatThrownBy(
            () -> new MappingInspector(" a = c1, b = c1, c = c2, d = c2", true, MAPPED_ONLY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("the following variables are mapped to more than one field: c1, c2");
  }

  @Test
  void should_error_out_same_writetime_function_mapped_twice_when_loading() {
    assertThatThrownBy(
            () -> new MappingInspector(" a = writetime(*), b = writetime(*)", true, MAPPED_ONLY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "the following variables are mapped to more than one field: writetime(*)");
  }

  @Test
  void should_error_out_same_ttl_function_mapped_twice_when_loading() {
    assertThatThrownBy(() -> new MappingInspector(" a = ttl(*), b = ttl(*)", true, MAPPED_ONLY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("the following variables are mapped to more than one field: ttl(*)");
  }

  @Test
  void should_detect_different_writetime_functions_when_loading() {
    MappingInspector inspector =
        new MappingInspector(" a = writetime(a), b = writetime(b,c)", true, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings())
        .containsEntry(
            new MappedMappingField("a"),
            new FunctionCall(null, WRITETIME, CQLWord.fromInternal("a")))
        .containsEntry(
            new MappedMappingField("b"),
            new FunctionCall(
                null, WRITETIME, CQLWord.fromInternal("b"), CQLWord.fromInternal("c")));
  }

  @Test
  void should_detect_different_ttl_functions_when_loading() {
    MappingInspector inspector =
        new MappingInspector(" a = ttl(a), b = ttl(b,c)", true, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings())
        .containsEntry(
            new MappedMappingField("a"), new FunctionCall(null, TTL, CQLWord.fromInternal("a")))
        .containsEntry(
            new MappedMappingField("b"),
            new FunctionCall(null, TTL, CQLWord.fromInternal("b"), CQLWord.fromInternal("c")));
  }

  @Test
  void should_accept_same_variable_mapped_twice_when_unloading() {
    MappingInspector inspector = new MappingInspector(" a = c1, b = c1", false, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings())
        .containsEntry(new MappedMappingField("a"), CQLWord.fromInternal("c1"))
        .containsEntry(new MappedMappingField("b"), CQLWord.fromInternal("c1"));
  }

  @Test
  void should_error_out_same_field_mapped_twice_when_unloading() {
    assertThatThrownBy(
            () -> new MappingInspector(" a = c1, a = c2, b = c3, b = c4", false, MAPPED_ONLY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("the following fields are mapped to more than one variable: a, b");
    assertThatThrownBy(
            () -> new MappingInspector(" a = c1, a = c2, b = c3, b = c4", false, MAPPED_ONLY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("the following fields are mapped to more than one variable: a, b");
  }

  @Test
  void should_accept_duplicate_mapping_when_loading_and_unloading() {
    MappingInspector inspector = new MappingInspector(" a = c1, a = c1", true, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings())
        .hasSize(1)
        .containsEntry(new MappedMappingField("a"), CQLWord.fromInternal("c1"));
    inspector = new MappingInspector(" a = c1, a = c1", false, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings())
        .hasSize(1)
        .containsEntry(new MappedMappingField("a"), CQLWord.fromInternal("c1"));
  }

  @Test
  void should_error_out_if_mapped_mapping_but_preference_is_indexed_only() {
    assertThatThrownBy(() -> new MappingInspector("* = *, a = b, now() = d", true, INDEXED_ONLY))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "Schema mapping contains named fields, but connector only supports indexed fields");
  }

  @Test
  @SuppressWarnings("deprecation")
  void should_error_when_mapped_ttl_but_no_using_ttl_variable() {
    assertThatThrownBy(
            () ->
                new MappingInspector(
                    "f1 = c1, f2 = __ttl", true, MAPPED_ONLY, () -> null, () -> null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "__ttl mapping token is not allowed when the companion query does not contain a USING TTL clause");
  }

  @Test
  @SuppressWarnings("deprecation")
  void should_error_when_mapped_timestamp_but_no_using_timestamp_variable() {
    assertThatThrownBy(
            () ->
                new MappingInspector(
                    "f1 = c1, f2 = __timestamp", true, MAPPED_ONLY, () -> null, () -> null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining(
            "__timestamp mapping token is not allowed when the companion query does not contain a USING TIMESTAMP clause");
  }

  @Test
  @SuppressWarnings("deprecation")
  void should_error_when_mapped_timestamp_but_not_writing() {
    assertThatThrownBy(
            () ->
                new MappingInspector(
                    "f1 = c1, f2 = __timestamp", false, MAPPED_ONLY, () -> null, () -> null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("__timestamp mapping token not allowed when unloading");
  }

  @Test
  @SuppressWarnings("deprecation")
  void should_error_when_mapped_ttl_but_not_writing() {
    assertThatThrownBy(
            () ->
                new MappingInspector(
                    "f1 = c1, f2 = __ttl", false, MAPPED_ONLY, () -> null, () -> null))
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("__ttl mapping token not allowed when unloading");
  }

  @Test
  void should_detect_qualified_function_name() {
    MappingInspector inspector = new MappingInspector("MyKs1 . NOW () = c1", true, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings())
        .containsEntry(
            new FunctionCall(CQLWord.fromInternal("MyKs1"), CQLWord.fromInternal("NOW")),
            CQLWord.fromInternal("c1"));
    inspector = new MappingInspector("\"My Ks1\".\"N O W\"() = c1", true, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings())
        .containsEntry(
            new FunctionCall(CQLWord.fromInternal("My Ks1"), CQLWord.fromInternal("N O W")),
            CQLWord.fromInternal("c1"));
    inspector = new MappingInspector("f1 = MyKs1 . PLUS ( C1 , C2 )", false, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings())
        .containsEntry(
            new MappedMappingField("f1"),
            new FunctionCall(
                CQLWord.fromInternal("MyKs1"),
                CQLWord.fromInternal("PLUS"),
                CQLWord.fromInternal("C1"),
                CQLWord.fromInternal("C2")));
    inspector =
        new MappingInspector("f1 = \"My Ks1\".\"P L U S\"(\"C 1\",\"C 2\")", false, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings())
        .containsEntry(
            new MappedMappingField("f1"),
            new FunctionCall(
                CQLWord.fromInternal("My Ks1"),
                CQLWord.fromInternal("P L U S"),
                CQLWord.fromInternal("C 1"),
                CQLWord.fromInternal("C 2")));
  }

  @Test
  void should_accept_same_ttl_and_writetime_as_valid_identifiers() {
    MappingInspector inspector =
        new MappingInspector(" ttl = ttl, writetime = writetime", true, MAPPED_ONLY);
    assertThat(inspector.getExplicitMappings())
        .containsEntry(new MappedMappingField("ttl"), CQLWord.fromInternal("ttl"))
        .containsEntry(new MappedMappingField("writetime"), CQLWord.fromInternal("writetime"));
  }
}
