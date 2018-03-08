/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.number;

import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;

import com.datastax.driver.core.TypeCodec;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.junit.jupiter.api.Test;

class NumberToNumberCodecTest {

  @Test
  void should_convert_when_valid_input() {

    assertThat(new NumberToNumberCodec<>(Byte.class, TypeCodec.cdouble()))
        .convertsFromExternal((byte) 123)
        .toInternal(123d)
        .convertsFromInternal(123d)
        .toExternal((byte) 123)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new NumberToNumberCodec<>(Byte.class, TypeCodec.varint()))
        .convertsFromExternal((byte) 123)
        .toInternal(new BigInteger("123"))
        .convertsFromInternal(new BigInteger("123"))
        .toExternal((byte) 123)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new NumberToNumberCodec<>(BigInteger.class, TypeCodec.cint()))
        .convertsFromExternal(new BigInteger("123456"))
        .toInternal(123456)
        .convertsFromInternal(123456)
        .toExternal(new BigInteger("123456"))
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new NumberToNumberCodec<>(Integer.class, TypeCodec.bigint()))
        .convertsFromExternal(123456)
        .toInternal(123456L)
        .convertsFromInternal(123456L)
        .toExternal(123456)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new NumberToNumberCodec<>(Float.class, TypeCodec.decimal()))
        .convertsFromExternal(-123.456f)
        .toInternal(new BigDecimal("-123.456"))
        .convertsFromInternal(new BigDecimal("-123.456"))
        .toExternal(-123.456f)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);

    assertThat(new NumberToNumberCodec<>(BigDecimal.class, TypeCodec.cint()))
        .convertsFromExternal(new BigDecimal("123"))
        .toInternal(123)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromInternal(null)
        .toExternal(null);
  }

  @Test
  void should_not_convert_when_invalid_input() {

    assertThat(new NumberToNumberCodec<>(Double.class, TypeCodec.cint()))
        .cannotConvertFromExternal(123.45d);

    assertThat(new NumberToNumberCodec<>(Long.class, TypeCodec.cint()))
        .cannotConvertFromExternal(Long.MAX_VALUE);

    assertThat(new NumberToNumberCodec<>(Short.class, TypeCodec.tinyInt()))
        .cannotConvertFromExternal((short) 1234);

    assertThat(new NumberToNumberCodec<>(BigInteger.class, TypeCodec.cint()))
        .cannotConvertFromExternal(new BigInteger("123000000000000000000000"));

    assertThat(new NumberToNumberCodec<>(BigDecimal.class, TypeCodec.cint()))
        .cannotConvertFromExternal(new BigDecimal("123.1"));
  }
}
