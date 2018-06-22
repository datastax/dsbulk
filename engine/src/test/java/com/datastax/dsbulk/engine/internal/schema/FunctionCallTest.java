/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.schema;

import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.ALIASED_SELECTOR;
import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.INTERNAL;
import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.NAMED_ASSIGNMENT;
import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.POSITIONAL_ASSIGNMENT;
import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.UNALIASED_SELECTOR;
import static com.datastax.dsbulk.engine.internal.schema.CQLRenderMode.VARIABLE;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import com.datastax.oss.driver.shaded.guava.common.collect.Lists;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FunctionCallTest {

  private static final CQLWord KS1 = CQLWord.fromInternal("ks1");
  private static final CQLWord KS1_UP = CQLWord.fromInternal("MyKs1");

  private static final CQLWord PLUS = CQLWord.fromInternal("plus");
  private static final CQLWord MAX = CQLWord.fromInternal("max");
  private static final CQLWord COL1 = CQLWord.fromInternal("col1");
  private static final CQLWord COL2 = CQLWord.fromInternal("col2");

  private static final CQLWord PLUS_UP = CQLWord.fromInternal("PLUS");
  private static final CQLWord MAX_UP = CQLWord.fromInternal("MAX");
  private static final CQLWord COL1_UP = CQLWord.fromInternal("My Col 1");
  private static final CQLWord COL2_UP = CQLWord.fromInternal("My Col 2");

  private static final CQLLiteral _42 = new CQLLiteral("42");

  private static final FunctionCall PLUS_FUNC =
      new FunctionCall(null, PLUS, COL1, new FunctionCall(null, MAX, COL2, _42));

  private static final FunctionCall PLUS_KS_FUNC =
      new FunctionCall(KS1, PLUS, COL1, new FunctionCall(KS1, MAX, COL2, _42));

  private static final FunctionCall PLUS_UP_FUNC =
      new FunctionCall(null, PLUS_UP, COL1_UP, new FunctionCall(null, MAX_UP, COL2_UP, _42));

  private static final FunctionCall PLUS_UP_KS_FUNC =
      new FunctionCall(KS1_UP, PLUS_UP, COL1_UP, new FunctionCall(KS1_UP, MAX_UP, COL2_UP, _42));

  @ParameterizedTest
  @MethodSource
  void should_render_function_call(FunctionCall functionCall, CQLRenderMode mode, String expected) {
    assertThat(functionCall.render(mode)).isEqualTo(expected);
  }

  @SuppressWarnings("unused")
  static List<Arguments> should_render_function_call() {
    return Lists.newArrayList(
        // INTERNAL
        arguments(PLUS_FUNC, INTERNAL, "plus(col1, max(col2, 42))"),
        arguments(PLUS_KS_FUNC, INTERNAL, "ks1.plus(col1, ks1.max(col2, 42))"),
        arguments(PLUS_UP_FUNC, INTERNAL, "PLUS(My Col 1, MAX(My Col 2, 42))"),
        arguments(PLUS_UP_KS_FUNC, INTERNAL, "MyKs1.PLUS(My Col 1, MyKs1.MAX(My Col 2, 42))"),
        // VARIABLE
        arguments(PLUS_FUNC, VARIABLE, "\"plus(col1, max(col2, 42))\""),
        arguments(PLUS_KS_FUNC, VARIABLE, "\"ks1.plus(col1, ks1.max(col2, 42))\""),
        arguments(PLUS_UP_FUNC, VARIABLE, "\"PLUS(My Col 1, MAX(My Col 2, 42))\""),
        arguments(PLUS_UP_KS_FUNC, VARIABLE, "\"MyKs1.PLUS(My Col 1, MyKs1.MAX(My Col 2, 42))\""),
        // NAMED_ASSIGNMENT
        arguments(PLUS_FUNC, NAMED_ASSIGNMENT, "plus(:col1, max(:col2, 42))"),
        arguments(PLUS_KS_FUNC, NAMED_ASSIGNMENT, "ks1.plus(:col1, ks1.max(:col2, 42))"),
        arguments(
            PLUS_UP_FUNC, NAMED_ASSIGNMENT, "\"PLUS\"(:\"My Col 1\", \"MAX\"(:\"My Col 2\", 42))"),
        arguments(
            PLUS_UP_KS_FUNC,
            NAMED_ASSIGNMENT,
            "\"MyKs1\".\"PLUS\"(:\"My Col 1\", \"MyKs1\".\"MAX\"(:\"My Col 2\", 42))"),
        // POSITIONAL_ASSIGNMENT
        arguments(PLUS_FUNC, POSITIONAL_ASSIGNMENT, "plus(?, max(?, 42))"),
        arguments(PLUS_KS_FUNC, POSITIONAL_ASSIGNMENT, "ks1.plus(?, ks1.max(?, 42))"),
        arguments(PLUS_UP_FUNC, POSITIONAL_ASSIGNMENT, "\"PLUS\"(?, \"MAX\"(?, 42))"),
        arguments(
            PLUS_UP_KS_FUNC,
            POSITIONAL_ASSIGNMENT,
            "\"MyKs1\".\"PLUS\"(?, \"MyKs1\".\"MAX\"(?, 42))"),
        // UNALIASED_SELECTOR
        arguments(PLUS_FUNC, UNALIASED_SELECTOR, "plus(col1, max(col2, 42))"),
        arguments(PLUS_KS_FUNC, UNALIASED_SELECTOR, "ks1.plus(col1, ks1.max(col2, 42))"),
        arguments(
            PLUS_UP_FUNC, UNALIASED_SELECTOR, "\"PLUS\"(\"My Col 1\", \"MAX\"(\"My Col 2\", 42))"),
        arguments(
            PLUS_UP_KS_FUNC,
            UNALIASED_SELECTOR,
            "\"MyKs1\".\"PLUS\"(\"My Col 1\", \"MyKs1\".\"MAX\"(\"My Col 2\", 42))"),
        // ALIASED_SELECTOR
        arguments(
            PLUS_FUNC,
            ALIASED_SELECTOR,
            "plus(col1, max(col2, 42)) AS \"plus(col1, max(col2, 42))\""),
        arguments(
            PLUS_KS_FUNC,
            ALIASED_SELECTOR,
            "ks1.plus(col1, ks1.max(col2, 42)) AS \"ks1.plus(col1, ks1.max(col2, 42))\""),
        arguments(
            PLUS_UP_FUNC,
            ALIASED_SELECTOR,
            "\"PLUS\"(\"My Col 1\", \"MAX\"(\"My Col 2\", 42)) AS \"PLUS(My Col 1, MAX(My Col 2, 42))\""),
        arguments(
            PLUS_UP_KS_FUNC,
            ALIASED_SELECTOR,
            "\"MyKs1\".\"PLUS\"(\"My Col 1\", \"MyKs1\".\"MAX\"(\"My Col 2\", 42)) AS \"MyKs1.PLUS(My Col 1, MyKs1.MAX(My Col 2, 42))\""));
  }
}
