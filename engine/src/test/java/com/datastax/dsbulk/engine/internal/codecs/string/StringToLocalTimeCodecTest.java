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
import static java.math.RoundingMode.HALF_EVEN;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.dsbulk.engine.internal.codecs.util.TemporalFormat;
import com.datastax.dsbulk.engine.internal.settings.CodecSettings;
import java.time.LocalTime;
import java.util.List;
import org.junit.jupiter.api.Test;

class StringToLocalTimeCodecTest {

  private TemporalFormat format1 =
      CodecSettings.getTemporalFormat(
          "ISO_LOCAL_TIME",
          null,
          US,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true));

  private TemporalFormat format2 =
      CodecSettings.getTemporalFormat(
          "HHmmss.SSS",
          null,
          US,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true));

  private final List<String> nullStrings = newArrayList("NULL");

  @Test
  void should_convert_from_valid_external() {
    StringToLocalTimeCodec codec = new StringToLocalTimeCodec(format1, UTC, nullStrings);
    assertThat(codec)
        .convertsFromExternal("12:24:46")
        .toInternal(LocalTime.parse("12:24:46"))
        .convertsFromExternal("12:24:46.999")
        .toInternal(LocalTime.parse("12:24:46.999"))
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null);
    codec = new StringToLocalTimeCodec(format2, UTC, nullStrings);
    assertThat(codec)
        .convertsFromExternal("122446.999")
        .toInternal(LocalTime.parse("12:24:46.999"));
  }

  @Test
  void should_convert_from_valid_internal() {
    StringToLocalTimeCodec codec = new StringToLocalTimeCodec(format1, UTC, nullStrings);
    assertThat(codec)
        .convertsFromInternal(LocalTime.parse("12:24:46.999"))
        .toExternal("12:24:46.999");
    codec = new StringToLocalTimeCodec(format2, UTC, nullStrings);
    assertThat(codec)
        .convertsFromInternal(LocalTime.parse("12:24:46.999"))
        .toExternal("122446.999");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    StringToLocalTimeCodec codec = new StringToLocalTimeCodec(format1, UTC, nullStrings);
    assertThat(codec).cannotConvertFromExternal("not a valid date format");
  }
}
