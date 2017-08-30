/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs;

import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import org.junit.Test;

public class StringToByteCodecTest {

  private StringToByteCodec codec =
      new StringToByteCodec(
          ThreadLocal.withInitial(
              () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))));

  @Test
  public void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom("0")
        .to((byte) 0)
        .convertsFrom("127")
        .to((byte) 127)
        .convertsFrom("-128")
        .to((byte) -128)
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo((byte) 0)
        .from("0")
        .convertsTo((byte) 127)
        .from("127")
        .convertsTo((byte) -128)
        .from("-128")
        .convertsTo(null)
        .from(null);
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec)
        .cannotConvertFrom("not a valid byte")
        .cannotConvertFrom("1.2")
        .cannotConvertFrom("128")
        .cannotConvertFrom("-129");
  }
}
