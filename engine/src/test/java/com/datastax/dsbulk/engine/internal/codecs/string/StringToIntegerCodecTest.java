/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.codecs.string;

import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static com.google.common.collect.Lists.newArrayList;
import static java.math.BigDecimal.ONE;
import static java.math.BigDecimal.ZERO;
import static java.math.RoundingMode.HALF_EVEN;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.dsbulk.engine.internal.codecs.util.CqlTemporalFormat;
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
          CqlTemporalFormat.DEFAULT_INSTANCE,
          UTC,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          ImmutableMap.of("true", true, "false", false),
          newArrayList(ONE, ZERO),
          newArrayList("NULL"));

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal("0")
        .toInternal(0)
        .convertsFromExternal("2147483647")
        .toInternal(Integer.MAX_VALUE)
        .convertsFromExternal("-2147483648")
        .toInternal(Integer.MIN_VALUE)
        .convertsFromExternal("2,147,483,647")
        .toInternal(Integer.MAX_VALUE)
        .convertsFromExternal("-2,147,483,648")
        .toInternal(Integer.MIN_VALUE)
        .convertsFromExternal("2,147,483,647")
        .toInternal(Integer.MAX_VALUE)
        .convertsFromExternal("-2,147,483,648")
        .toInternal(Integer.MIN_VALUE)
        .convertsFromExternal("1970-01-01T00:00:00Z")
        .toInternal(0)
        .convertsFromExternal("TRUE")
        .toInternal(1)
        .convertsFromExternal("FALSE")
        .toInternal(0)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec)
        .convertsFromInternal(0)
        .toExternal("0")
        .convertsFromInternal(Integer.MAX_VALUE)
        .toExternal("2,147,483,647")
        .convertsFromInternal(Integer.MIN_VALUE)
        .toExternal("-2,147,483,648")
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec)
        .cannotConvertFromExternal("")
        .cannotConvertFromExternal("not a valid integer")
        .cannotConvertFromExternal("1.2")
        .cannotConvertFromExternal("2147483648")
        .cannotConvertFromExternal("-2147483649")
        .cannotConvertFromExternal("2000-01-01T00:00:00Z") // overflow
    ;
  }
}
