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
import io.netty.util.concurrent.FastThreadLocal;
import java.math.RoundingMode;
import java.text.NumberFormat;
import org.junit.jupiter.api.Test;

class StringToFloatCodecTest {

  private final FastThreadLocal<NumberFormat> numberFormat =
      CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  private final StringToFloatCodec codec =
      new StringToFloatCodec(
          numberFormat,
          OverflowStrategy.REJECT,
          RoundingMode.HALF_EVEN,
          CQL_DATE_TIME_FORMAT,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          ImmutableMap.of("true", true, "false", false),
          newArrayList(ONE, ZERO),
          newArrayList("NULL"));

  @Test
  @SuppressWarnings("FloatingPointLiteralPrecision")
  void should_convert_from_valid_input() {
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
        .convertsFrom("340,282,350,000,000,000,000,000,000,000,000,000,000")
        .to(Float.MAX_VALUE)
        .convertsFrom("0.0000000000000000000000000000000000000000000014")
        .to(Float.MIN_VALUE)
        .convertsFrom("1970-01-01T00:00:00Z")
        .to(0f)
        .convertsFrom("TRUE")
        .to(1f)
        .convertsFrom("FALSE")
        .to(0f)
        .convertsFrom(null)
        .to(null)
        .convertsFrom("NULL")
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() {
    assertThat(codec)
        .convertsTo(0f)
        .from("0")
        .convertsTo(1234.56f)
        .from("1,234.56")
        .convertsTo(Float.MAX_VALUE)
        .from("340,282,350,000,000,000,000,000,000,000,000,000,000")
        .convertsTo(0.001f)
        .from("0") // decimals truncated
        .convertsTo(null)
        .from("NULL");
  }

  @Test
  void should_not_convert_from_invalid_input() {
    assertThat(codec).cannotConvertFrom("not a valid float");
  }
}
