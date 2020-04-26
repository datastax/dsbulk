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
package com.datastax.oss.dsbulk.codecs.jdk.number;

import static com.datastax.oss.driver.shaded.guava.common.collect.Lists.newArrayList;
import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static java.math.BigDecimal.ONE;

import org.junit.jupiter.api.Test;

class NumberToBooleanCodecTest {

  @Test
  void should_convert_from_valid_internal() {

    assertThat(new NumberToBooleanCodec<>(Byte.class, newArrayList(ONE, ONE.negate())))
        .convertsFromExternal((byte) 1)
        .toInternal(true)
        .convertsFromExternal((byte) -1)
        .toInternal(false)
        .convertsFromInternal(true)
        .toExternal((byte) 1)
        .convertsFromInternal(false)
        .toExternal((byte) -1)
        .cannotConvertFromExternal((byte) 0)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }
}
