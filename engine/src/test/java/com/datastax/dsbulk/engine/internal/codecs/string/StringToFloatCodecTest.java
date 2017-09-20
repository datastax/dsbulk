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

public class StringToFloatCodecTest {

  private StringToFloatCodec codec =
      new StringToFloatCodec(
          ThreadLocal.withInitial(
              () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))));

  @Test
  public void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom("0")
        .to(0f)
        .convertsFrom("1234.56")
        .to(1234.56f)
        .convertsFrom("1,234.56")
        .to(1234.56f)
        .convertsFrom("3.4028235E38")
        .to(Float.MAX_VALUE)
        .convertsFrom("1.4E-45")
        .to(Float.MIN_VALUE)
        .convertsFrom("340,282,346,638,528,860,000,000,000,000,000,000,000")
        .to(Float.MAX_VALUE)
        .convertsFrom("0.0000000000000000000000000000000000000000000014")
        .to(Float.MIN_VALUE)
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(0f)
        .from("0")
        .convertsTo(1234.56f)
        .from("1,234.56")
        .convertsTo(Float.MAX_VALUE)
        .from("340,282,346,638,528,860,000,000,000,000,000,000,000")
        .convertsTo(0.001f)
        .from("0") // decimals truncated
        .convertsTo(null)
        .from(null);
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec).cannotConvertFrom("not a valid float");
  }
}
