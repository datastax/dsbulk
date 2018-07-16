/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.number;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;

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
