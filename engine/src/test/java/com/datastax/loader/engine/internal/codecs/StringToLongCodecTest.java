/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import com.datastax.driver.core.exceptions.InvalidTypeException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import org.junit.Test;

import static com.datastax.driver.core.ProtocolVersion.V4;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;

public class StringToLongCodecTest {

  private ThreadLocal<DecimalFormat> formatter =
      ThreadLocal.withInitial(
          () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US)));

  @Test
  public void should_serialize_when_valid_input() throws Exception {
    StringToLongCodec codec = new StringToLongCodec(formatter);
    assertSerde(codec, "0");
    assertSerde(codec, formatter.get().format(Long.MAX_VALUE));
    assertSerde(codec, formatter.get().format(Long.MIN_VALUE));
  }

  @Test
  public void should_not_serialize_when_invalid_input() throws Exception {
    StringToLongCodec codec = new StringToLongCodec(formatter);
    try {
      assertSerde(codec, formatter.get().format(0.12345));
      fail("Expecting InvalidTypeException");
    } catch (InvalidTypeException ignored) {
    }
  }

  private void assertSerde(StringToLongCodec codec, String input) {
    assertThat(codec.deserialize(codec.serialize(input, V4), V4)).isEqualTo(input);
  }
}
