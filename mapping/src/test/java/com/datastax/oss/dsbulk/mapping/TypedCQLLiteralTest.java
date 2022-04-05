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

import static org.assertj.core.api.Assertions.assertThat;

import com.datastax.oss.driver.api.core.type.DataTypes;
import org.junit.jupiter.api.Test;

class TypedCQLLiteralTest {

  @Test
  void should_render() {
    TypedCQLLiteral literal = new TypedCQLLiteral("123", DataTypes.INT);
    assertThat(literal.render(CQLRenderMode.UNALIASED_SELECTOR)).isEqualTo("(int)123");
    assertThat(literal.render(CQLRenderMode.ALIASED_SELECTOR))
        .isEqualTo("(int)123 AS \"(int)123\"");
    assertThat(literal.render(CQLRenderMode.INTERNAL)).isEqualTo("(int)123");
    assertThat(literal.render(CQLRenderMode.NAMED_ASSIGNMENT)).isEqualTo("123");
    assertThat(literal.render(CQLRenderMode.POSITIONAL_ASSIGNMENT)).isEqualTo("123");
  }
}
