/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import static com.datastax.loader.engine.internal.Assertions.assertThat;

import com.datastax.driver.core.TypeCodec;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.junit.Test;

public class NumberToNumberCodecTest {

  @Test
  public void should_convert_when_valid_input() throws Exception {

    assertThat(new NumberToNumberCodec<>(Byte.class, TypeCodec.cdouble()))
        .convertsFrom((byte) 123)
        .to(123d)
        .convertsTo(123d)
        .from((byte) 123)
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(new NumberToNumberCodec<>(Byte.class, TypeCodec.varint()))
        .convertsFrom((byte) 123)
        .to(new BigInteger("123"))
        .convertsTo(new BigInteger("123"))
        .from((byte) 123)
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(new NumberToNumberCodec<>(BigInteger.class, TypeCodec.cint()))
        .convertsFrom(new BigInteger("123456"))
        .to(123456)
        .convertsTo(123456)
        .from(new BigInteger("123456"))
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(new NumberToNumberCodec<>(Integer.class, TypeCodec.bigint()))
        .convertsFrom(123456)
        .to(123456L)
        .convertsTo(123456L)
        .from(123456)
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(new NumberToNumberCodec<>(Float.class, TypeCodec.decimal()))
        .convertsFrom(-123.456f)
        .to(new BigDecimal("-123.456"))
        .convertsTo(new BigDecimal("-123.456"))
        .from(-123.456f)
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);

    assertThat(new NumberToNumberCodec<>(BigDecimal.class, TypeCodec.cint()))
        .convertsFrom(new BigDecimal("123"))
        .to(123)
        .convertsFrom(null)
        .to(null)
        .convertsTo(null)
        .from(null);
  }

  @Test
  public void should_not_convert_when_invalid_input() throws Exception {

    assertThat(new NumberToNumberCodec<>(Double.class, TypeCodec.cint()))
        .cannotConvertFrom(123.45d);

    assertThat(new NumberToNumberCodec<>(Long.class, TypeCodec.cint()))
        .cannotConvertFrom(Long.MAX_VALUE);

    assertThat(new NumberToNumberCodec<>(Short.class, TypeCodec.tinyInt()))
        .cannotConvertFrom((short) 1234);

    assertThat(new NumberToNumberCodec<>(BigInteger.class, TypeCodec.cint()))
        .cannotConvertFrom(new BigInteger("123000000000000000000000"));

    assertThat(new NumberToNumberCodec<>(BigDecimal.class, TypeCodec.cint()))
        .cannotConvertFrom(new BigDecimal("123.1"));
  }
}
