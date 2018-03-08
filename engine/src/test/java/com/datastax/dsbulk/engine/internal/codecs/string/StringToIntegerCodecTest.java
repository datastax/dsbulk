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

class StringToIntegerCodecTest {

  private final FastThreadLocal<NumberFormat> numberFormat =
      CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  private final StringToIntegerCodec codec =
      new StringToIntegerCodec(
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
  void should_convert_from_valid_input() {
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
        .convertsFrom("TRUE")
        .to(1)
        .convertsFrom("FALSE")
        .to(0)
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
        .convertsTo(0)
        .from("0")
        .convertsTo(Integer.MAX_VALUE)
        .from("2,147,483,647")
        .convertsTo(Integer.MIN_VALUE)
        .from("-2,147,483,648")
        .convertsTo(null)
        .from("NULL");
  }

  @Test
  void should_not_convert_from_invalid_input() {
    assertThat(codec)
        .cannotConvertFrom("not a valid integer")
        .cannotConvertFrom("1.2")
        .cannotConvertFrom("2147483648")
        .cannotConvertFrom("-2147483649")
        .cannotConvertFrom("2000-01-01T00:00:00Z") // overflow
    ;
  }
}
