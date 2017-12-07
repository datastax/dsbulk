/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.internal.EngineAssertions.assertThat;
import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static java.time.Instant.EPOCH;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.google.common.collect.ImmutableMap;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import org.junit.jupiter.api.Test;

class StringToBigDecimalCodecTest {

  private final StringToBigDecimalCodec codec =
      new StringToBigDecimalCodec(
          ThreadLocal.withInitial(
              () -> new DecimalFormat("#,###.##", DecimalFormatSymbols.getInstance(Locale.US))),
          CQL_DATE_TIME_FORMAT,
          MILLISECONDS,
          EPOCH,
          ImmutableMap.of("true", true, "false", false),
          newArrayList(ONE, ZERO));

  @Test
  void should_convert_from_valid_input() {
    assertThat(codec)
        .convertsFrom("0")
        .to(ZERO)
        .convertsFrom("-1234.56")
        .to(new BigDecimal("-1234.56"))
        .convertsFrom("1970-01-01T00:00:00Z")
        .to(new BigDecimal("0"))
        .convertsFrom("2000-01-01T00:00:00Z")
        .to(new BigDecimal("946684800000"))
        .convertsFrom("true")
        .to(new BigDecimal("1"))
        .convertsFrom("false")
        .to(new BigDecimal("0"))
        .convertsFrom("TRUE")
        .to(ONE)
        .convertsFrom("FALSE")
        .to(ZERO)
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() {
    assertThat(codec)
        .convertsTo(ZERO)
        .from("0")
        .convertsTo(new BigDecimal("1234.56"))
        .from("1,234.56")
        .convertsTo(null)
        .from(null);
  }

  @Test
  void should_not_convert_from_invalid_input() {
    assertThat(codec).cannotConvertFrom("not a valid decimal");
  }
}
