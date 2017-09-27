/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import org.junit.Test;

public class StringToDoubleCodecTest {

  private StringToDoubleCodec codec =
      new StringToDoubleCodec(
          ThreadLocal.withInitial(
              () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))));

  @Test
  public void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom("0")
        .to(0d)
        .convertsFrom("1234.56")
        .to(1234.56d)
        .convertsFrom("1,234.56")
        .to(1234.56d)
        .convertsFrom("1.7976931348623157E308")
        .to(Double.MAX_VALUE)
        .convertsFrom("4.9E-324")
        .to(Double.MIN_VALUE)
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(0d)
        .from("0")
        .convertsTo(1234.56d)
        .from("1,234.56")
        .convertsTo(0.001)
        .from("0") // decimals truncated
        .convertsTo(null)
        .from(null);
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec).cannotConvertFrom("not a valid double");
  }
}
