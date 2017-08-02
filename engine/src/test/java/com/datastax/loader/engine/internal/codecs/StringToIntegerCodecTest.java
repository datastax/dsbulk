/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import static com.datastax.loader.engine.internal.codecs.ConvertingCodecAssert.assertThat;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import org.junit.Test;

public class StringToIntegerCodecTest {

  private StringToIntegerCodec codec =
      new StringToIntegerCodec(
          ThreadLocal.withInitial(
              () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))));

  @Test
  public void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom("0")
        .to(0)
        .convertsFrom("2147483647")
        .to(Integer.MAX_VALUE)
        .convertsFrom("-2147483648")
        .to(Integer.MIN_VALUE)
        .convertsFrom("2,147,483,647")
        .to(Integer.MAX_VALUE)
        .convertsFrom("-2,147,483,648")
        .to(Integer.MIN_VALUE)
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(0)
        .from("0")
        .convertsTo(Integer.MAX_VALUE)
        .from("2,147,483,647")
        .convertsTo(Integer.MIN_VALUE)
        .from("-2,147,483,648")
        .convertsTo(null)
        .from(null);
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec)
        .cannotConvertFrom("not a valid integer")
        .cannotConvertFrom("1.2")
        .cannotConvertFrom("2147483648")
        .cannotConvertFrom("-2147483649");
  }
}
