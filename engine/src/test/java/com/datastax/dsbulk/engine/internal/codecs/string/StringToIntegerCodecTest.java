/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.internal.EngineAssertions.assertThat;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import org.junit.jupiter.api.Test;

class StringToIntegerCodecTest {

  private final StringToIntegerCodec codec =
      new StringToIntegerCodec(
          ThreadLocal.withInitial(
              () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))),
          CQL_DATE_TIME_FORMAT,
          MILLISECONDS,
          EPOCH);

  @Test
  void should_convert_from_valid_input() throws Exception {
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
        .convertsFrom("2,147,483,647")
        .to(Integer.MAX_VALUE)
        .convertsFrom("-2,147,483,648")
        .to(Integer.MIN_VALUE)
        .convertsFrom("1970-01-01T00:00:00Z")
        .to(0)
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() throws Exception {
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
  void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec)
        .cannotConvertFrom("not a valid integer")
        .cannotConvertFrom("1.2")
        .cannotConvertFrom("2147483648")
        .cannotConvertFrom("-2147483649")
        .cannotConvertFrom("2000-01-01T00:00:00Z") // overflow
    ;
  }
}
