/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static java.math.RoundingMode.HALF_EVEN;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.dsbulk.engine.internal.codecs.util.OverflowStrategy;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import com.google.common.collect.ImmutableMap;
import java.math.RoundingMode;
import java.text.DecimalFormat;
import org.junit.jupiter.api.Test;

class StringToDoubleCodecTest {

  private final ThreadLocal<DecimalFormat> numberFormat =
      ThreadLocal.withInitial(() -> CodecSettings.getNumberFormat("#,###.##", US, HALF_EVEN));

  private final StringToDoubleCodec codec =
      new StringToDoubleCodec(
          numberFormat,
          OverflowStrategy.REJECT,
          RoundingMode.HALF_EVEN,
          CQL_DATE_TIME_FORMAT,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          ImmutableMap.of("true", true, "false", false),
          newArrayList(ONE, ZERO));

  @Test
  void should_convert_from_valid_input() {
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
        .convertsFrom("TRUE")
        .to(1d)
        .convertsFrom("FALSE")
        .to(0d)
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() {
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
  void should_not_convert_from_invalid_input() {
    assertThat(codec).cannotConvertFrom("not a valid double");
  }
}
