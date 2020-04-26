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

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;

import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.junit.jupiter.api.Test;

class NumberToNumberCodecTest {

  @Test
  void should_convert_when_valid_input() {

    assertThat(new NumberToNumberCodec<>(Byte.class, TypeCodecs.DOUBLE))
        .convertsFromExternal((byte) 123)
        .toInternal(123d)
        .convertsFromInternal(123d)
        .toExternal((byte) 123)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new NumberToNumberCodec<>(Byte.class, TypeCodecs.VARINT))
        .convertsFromExternal((byte) 123)
        .toInternal(new BigInteger("123"))
        .convertsFromInternal(new BigInteger("123"))
        .toExternal((byte) 123)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new NumberToNumberCodec<>(BigInteger.class, TypeCodecs.INT))
        .convertsFromExternal(new BigInteger("123456"))
        .toInternal(123456)
        .convertsFromInternal(123456)
        .toExternal(new BigInteger("123456"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new NumberToNumberCodec<>(Integer.class, TypeCodecs.BIGINT))
        .convertsFromExternal(123456)
        .toInternal(123456L)
        .convertsFromInternal(123456L)
        .toExternal(123456)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new NumberToNumberCodec<>(Float.class, TypeCodecs.DECIMAL))
        .convertsFromExternal(-123.456f)
        .toInternal(new BigDecimal("-123.456"))
        .convertsFromInternal(new BigDecimal("-123.456"))
        .toExternal(-123.456f)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new NumberToNumberCodec<>(BigDecimal.class, TypeCodecs.INT))
        .convertsFromExternal(new BigDecimal("123"))
        .toInternal(123)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_when_invalid_input() {

    assertThat(new NumberToNumberCodec<>(Double.class, TypeCodecs.INT))
        .cannotConvertFromExternal(123.45d);

    assertThat(new NumberToNumberCodec<>(Long.class, TypeCodecs.INT))
        .cannotConvertFromExternal(Long.MAX_VALUE);

    assertThat(new NumberToNumberCodec<>(Short.class, TypeCodecs.TINYINT))
        .cannotConvertFromExternal((short) 1234);

    assertThat(new NumberToNumberCodec<>(BigInteger.class, TypeCodecs.INT))
        .cannotConvertFromExternal(new BigInteger("123000000000000000000000"));

    assertThat(new NumberToNumberCodec<>(BigDecimal.class, TypeCodecs.INT))
        .cannotConvertFromExternal(new BigDecimal("123.1"));
  }
}
