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

class StringToFloatCodecTest {

  private final FastThreadLocal<NumberFormat> numberFormat =
      CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true);

  private final StringToFloatCodec codec =
      new StringToFloatCodec(
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
  @SuppressWarnings("FloatingPointLiteralPrecision")
  void should_convert_from_valid_external() {
    assertThat(codec)
        .convertsFromExternal("0")
        .toInternal(0f)
        .convertsFromExternal("1234.56")
        .toInternal(1234.56f)
        .convertsFromExternal("1,234.56")
        .toInternal(1234.56f)
        .convertsFromExternal("3.4028235E38")
        .toInternal(Float.MAX_VALUE)
        .convertsFromExternal("1.4E-45")
        .toInternal(Float.MIN_VALUE)
        .convertsFromExternal("340,282,350,000,000,000,000,000,000,000,000,000,000")
        .toInternal(Float.MAX_VALUE)
        .convertsFromExternal("0.0000000000000000000000000000000000000000000014")
        .toInternal(Float.MIN_VALUE)
        .convertsFromExternal("1970-01-01T00:00:00Z")
        .toInternal(0f)
        .convertsFromExternal("TRUE")
        .toInternal(1f)
        .convertsFromExternal("FALSE")
        .toInternal(0f)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec)
        .convertsFromInternal(0f)
        .toExternal("0")
        .convertsFromInternal(1234.56f)
        .toExternal("1,234.56")
        .convertsFromInternal(Float.MAX_VALUE)
        .toExternal("340,282,350,000,000,000,000,000,000,000,000,000,000")
        .convertsFromInternal(0.001f)
        .toExternal("0") // decimals truncated
        .convertsFromInternal(null)
        .toExternal("NULL");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec).cannotConvertFromExternal("").cannotConvertFromExternal("not a valid float");
  }
}
