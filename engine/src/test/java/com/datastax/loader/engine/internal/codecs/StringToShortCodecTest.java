/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import static com.datastax.driver.core.ProtocolVersion.V4;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import org.junit.Test;

public class StringToShortCodecTest {

  private ThreadLocal<DecimalFormat> formatter =
      ThreadLocal.withInitial(
          () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US)));

  @Test
  public void should_serialize_when_valid_input() throws Exception {
    StringToShortCodec codec = new StringToShortCodec(formatter);
    assertSerde(codec, "0");
    assertSerde(codec, formatter.get().format(Short.MAX_VALUE));
    assertSerde(codec, formatter.get().format(Short.MIN_VALUE));
  }

  @Test
  public void should_not_serialize_when_invalid_input() throws Exception {
    StringToShortCodec codec = new StringToShortCodec(formatter);
    try {
      assertSerde(codec, formatter.get().format(Short.MAX_VALUE + 1));
      fail("Expecting InvalidTypeException");
    } catch (InvalidTypeException ignored) {
    }
    try {
      assertSerde(codec, formatter.get().format(Short.MIN_VALUE - 1));
      fail("Expecting InvalidTypeException");
    } catch (InvalidTypeException ignored) {
    }
  }

  private void assertSerde(StringToShortCodec codec, String input) {
    assertThat(codec.deserialize(codec.serialize(input, V4), V4)).isEqualTo(input);
  }
}
