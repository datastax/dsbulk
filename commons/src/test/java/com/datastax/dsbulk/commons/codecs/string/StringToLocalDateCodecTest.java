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
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.datastax.dsbulk.commons.codecs.util.CodecUtils;
import com.datastax.dsbulk.commons.codecs.util.TemporalFormat;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.List;
import org.junit.jupiter.api.Test;

class StringToLocalDateCodecTest {

  private TemporalFormat format1 =
      CodecUtils.getTemporalFormat(
          "ISO_LOCAL_DATE",
          UTC,
          US,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          CodecUtils.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true),
          false);

  private TemporalFormat format2 =
      CodecUtils.getTemporalFormat(
          "yyyyMMdd",
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
          DAYS,
          ZonedDateTime.parse("2000-01-01T00:00:00Z"),
          CodecUtils.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true),
          false);

  private final List<String> nullStrings = newArrayList("NULL");

  @Test
  void should_convert_from_valid_external() {
    StringToLocalDateCodec codec = new StringToLocalDateCodec(format1, UTC, nullStrings);
    assertThat(codec)
        .convertsFromExternal("2016-07-24")
        .toInternal(LocalDate.parse("2016-07-24"))
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null);
    codec = new StringToLocalDateCodec(format2, UTC, nullStrings);
    assertThat(codec).convertsFromExternal("20160724").toInternal(LocalDate.parse("2016-07-24"));
    codec = new StringToLocalDateCodec(format3, UTC, nullStrings);
    // 12 full days after year 2000 = 2000-01-13 (at midnight)
    assertThat(codec).convertsFromExternal("12").toInternal(LocalDate.parse("2000-01-13"));
  }

  @Test
  void should_convert_from_valid_internal() {
    StringToLocalDateCodec codec = new StringToLocalDateCodec(format1, UTC, nullStrings);
    assertThat(codec).convertsFromInternal(LocalDate.parse("2016-07-24")).toExternal("2016-07-24");
    codec = new StringToLocalDateCodec(format2, UTC, nullStrings);
    assertThat(codec).convertsFromInternal(LocalDate.parse("2016-07-24")).toExternal("20160724");
    codec = new StringToLocalDateCodec(format3, UTC, nullStrings);
    assertThat(codec).convertsFromInternal(LocalDate.parse("2000-01-13")).toExternal("12");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    StringToLocalDateCodec codec = new StringToLocalDateCodec(format1, UTC, nullStrings);
    assertThat(codec).cannotConvertFromExternal("not a valid date format");
  }
}
