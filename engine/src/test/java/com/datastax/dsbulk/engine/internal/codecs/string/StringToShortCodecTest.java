/*
 * Copyright DataStax Inc.
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

public class StringToShortCodecTest {

  private StringToShortCodec codec =
      new StringToShortCodec(
          ThreadLocal.withInitial(
              () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))));

  @Test
  public void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom("0")
        .to((short) 0)
        .convertsFrom("32767")
        .to((short) 32767)
        .convertsFrom("-32768")
        .to((short) -32768)
        .convertsFrom("32,767")
        .to((short) 32767)
        .convertsFrom("-32,768")
        .to((short) -32768)
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo((short) 0)
        .from("0")
        .convertsTo((short) 32767)
        .from("32,767")
        .convertsTo((short) -32768)
        .from("-32,768")
        .convertsTo(null)
        .from(null);
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec)
        .cannotConvertFrom("not a valid short")
        .cannotConvertFrom("1.2")
        .cannotConvertFrom("32768")
        .cannotConvertFrom("-32769");
  }
}
