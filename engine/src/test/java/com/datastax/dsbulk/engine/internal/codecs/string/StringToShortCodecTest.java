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
import java.text.NumberFormat;
import org.junit.jupiter.api.Test;

class StringToShortCodecTest {

  private final ThreadLocal<NumberFormat> numberFormat =
      ThreadLocal.withInitial(() -> CodecSettings.getNumberFormat("#,###.##", US, HALF_EVEN, true));

  private final StringToShortCodec codec =
      new StringToShortCodec(
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
        .to((short) 0)
        .convertsFrom("32767")
        .to((short) 32767)
        .convertsFrom("-32768")
        .to((short) -32768)
        .convertsFrom("32,767")
        .to((short) 32767)
        .convertsFrom("-32,768")
        .to((short) -32768)
        .convertsFrom("1970-01-01T00:00:00Z")
        .to((short) 0)
        .convertsFrom("TRUE")
        .to((short) 1)
        .convertsFrom("FALSE")
        .to((short) 0)
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  void should_convert_to_valid_input() {
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
  void should_not_convert_from_invalid_input() {
    assertThat(codec)
        .cannotConvertFrom("not a valid short")
        .cannotConvertFrom("1.2")
        .cannotConvertFrom("32768")
        .cannotConvertFrom("-32769")
        .cannotConvertFrom("2000-01-01T00:00:00Z") // overflow
    ;
  }
}
