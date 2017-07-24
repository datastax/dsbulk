/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.exceptions.InvalidTypeException;
import java.math.BigDecimal;
import java.math.BigInteger;
import org.junit.Test;

import static com.datastax.driver.core.ProtocolVersion.V4;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class NumberToNumberCodecTest {

  @Test
  public void should_serialize_when_valid_input() throws Exception {
    assertSerde(new NumberToNumberCodec<>(Byte.class, TypeCodec.cdouble()), (byte) 123);
    assertSerde(new NumberToNumberCodec<>(Byte.class, TypeCodec.varint()), (byte) 123);
    assertSerde(new NumberToNumberCodec<>(Byte.class, TypeCodec.decimal()), (byte) 123);
    assertSerde(new NumberToNumberCodec<>(Double.class, TypeCodec.cint()), 123d);
    assertSerde(new NumberToNumberCodec<>(Short.class, TypeCodec.tinyInt()), (short) 123);
    assertSerde(
        new NumberToNumberCodec<>(BigInteger.class, TypeCodec.cint()), new BigInteger("123"));
    assertSerde(
        new NumberToNumberCodec<>(BigDecimal.class, TypeCodec.cint()), new BigDecimal("123"));
  }

  @Test
  public void should_not_serialize_when_invalid_input() throws Exception {
    try {
      assertSerde(new NumberToNumberCodec<>(Double.class, TypeCodec.cint()), 123.45d);
      fail("Expecting InvalidTypeException");
    } catch (InvalidTypeException ignored) {
    }
    try {
      assertSerde(
          new NumberToNumberCodec<>(Double.class, TypeCodec.cint()), (double) Long.MAX_VALUE);
      fail("Expecting InvalidTypeException");
    } catch (InvalidTypeException ignored) {
    }
    try {
      assertSerde(new NumberToNumberCodec<>(Short.class, TypeCodec.tinyInt()), (short) 1234);
      fail("Expecting InvalidTypeException");
    } catch (InvalidTypeException ignored) {
    }
    try {
      assertSerde(
          new NumberToNumberCodec<>(BigInteger.class, TypeCodec.cint()),
          new BigInteger("123000000000000000000000"));
      fail("Expecting InvalidTypeException");
    } catch (InvalidTypeException ignored) {
    }
    try {
      assertSerde(
          new NumberToNumberCodec<>(BigDecimal.class, TypeCodec.cint()), new BigDecimal("123.456"));
      fail("Expecting InvalidTypeException");
    } catch (InvalidTypeException ignored) {
    }
  }

  private <FROM extends Number, TO extends Number> void assertSerde(
      NumberToNumberCodec<FROM, TO> codec, FROM input) {
    assertThat(codec.deserialize(codec.serialize(input, V4), V4)).isEqualTo(input);
  }
}
