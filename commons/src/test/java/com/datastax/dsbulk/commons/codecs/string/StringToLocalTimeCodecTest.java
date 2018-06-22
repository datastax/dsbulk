/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.commons.codecs.string;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static com.datastax.oss.driver.shaded.guava.common.collect.Lists.newArrayList;
import static java.math.RoundingMode.HALF_EVEN;
import static java.time.Instant.EPOCH;
import static java.time.ZoneOffset.UTC;
import static java.util.Locale.US;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.datastax.dsbulk.commons.codecs.util.CodecUtils;
import com.datastax.dsbulk.commons.codecs.util.TemporalFormat;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;

class StringToLocalTimeCodecTest {

  private TemporalFormat format1 =
      CodecUtils.getTemporalFormat(
          "ISO_LOCAL_TIME",
          UTC,
          US,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          CodecUtils.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true),
          false);

  private TemporalFormat format2 =
      CodecUtils.getTemporalFormat(
          "HHmmss.SSS",
          UTC,
          US,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          CodecUtils.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true),
          false);

  private TemporalFormat format3 =
      CodecUtils.getTemporalFormat(
          "UNITS_SINCE_EPOCH",
          UTC,
          US,
          MINUTES,
          ZonedDateTime.parse("2000-01-01T00:00:00Z"),
          CodecUtils.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true),
          false);

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
    codec = new StringToLocalTimeCodec(format3, UTC, nullStrings);
    // 123 minutes after year 2000 = 02:03:00
    assertThat(codec).convertsFromExternal("123").toInternal(LocalTime.parse("02:03:00"));
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
    codec = new StringToLocalTimeCodec(format3, UTC, nullStrings);
    assertThat(codec).convertsFromInternal(LocalTime.parse("02:03:00")).toExternal("123");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    StringToLocalTimeCodec codec = new StringToLocalTimeCodec(format1, UTC, nullStrings);
    assertThat(codec).cannotConvertFromExternal("not a valid date format");
  }
}
