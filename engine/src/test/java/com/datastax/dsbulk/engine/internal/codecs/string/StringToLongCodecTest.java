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

public class StringToLongCodecTest {

  private StringToLongCodec codec =
      new StringToLongCodec(
          ThreadLocal.withInitial(
              () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))));

  @Test
  public void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom("0")
        .to(0L)
        .convertsFrom("9223372036854775807")
        .to(Long.MAX_VALUE)
        .convertsFrom("-9223372036854775808")
        .to(Long.MIN_VALUE)
        .convertsFrom("9,223,372,036,854,775,807")
        .to(Long.MAX_VALUE)
        .convertsFrom("-9,223,372,036,854,775,808")
        .to(Long.MIN_VALUE)
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(0L)
        .from("0")
        .convertsTo(Long.MAX_VALUE)
        .from("9,223,372,036,854,775,807")
        .convertsTo(Long.MIN_VALUE)
        .from("-9,223,372,036,854,775,808")
        .convertsTo(null)
        .from(null);
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec)
        .cannotConvertFrom("not a valid long")
        .cannotConvertFrom("1.2")
        .cannotConvertFrom("9223372036854775808")
        .cannotConvertFrom("-9223372036854775809");
  }
}
