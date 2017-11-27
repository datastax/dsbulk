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

class StringToDoubleCodecTest {

  private final StringToDoubleCodec codec =
      new StringToDoubleCodec(
          ThreadLocal.withInitial(
              () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))),
          CQL_DATE_TIME_FORMAT,
          MILLISECONDS,
          EPOCH);

  @Test
  void should_convert_from_valid_input() throws Exception {
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
        .convertsFrom("1970-01-01T00:00:00Z")
        .to(0d)
        .convertsFrom("2000-01-01T00:00:00Z")
        .to(946684800000d)
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() throws Exception {
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
  void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec).cannotConvertFrom("not a valid double");
  }
}
