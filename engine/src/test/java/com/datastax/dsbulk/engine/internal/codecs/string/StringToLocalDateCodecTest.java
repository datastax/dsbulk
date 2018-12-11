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
import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.Test;

class StringToLocalDateCodecTest {

  private TemporalFormat format1 =
      CodecSettings.getTemporalFormat(
          "ISO_LOCAL_DATE",
          null,
          US,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true));

  private TemporalFormat format2 =
      CodecSettings.getTemporalFormat(
          "yyyyMMdd",
          null,
          US,
          MILLISECONDS,
          EPOCH.atZone(UTC),
          CodecSettings.getNumberFormatThreadLocal("#,###.##", US, HALF_EVEN, true));

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
  }

  @Test
  void should_convert_from_valid_internal() {
    StringToLocalDateCodec codec = new StringToLocalDateCodec(format1, UTC, nullStrings);
    assertThat(codec).convertsFromInternal(LocalDate.parse("2016-07-24")).toExternal("2016-07-24");
    codec = new StringToLocalDateCodec(format2, UTC, nullStrings);
    assertThat(codec).convertsFromInternal(LocalDate.parse("2016-07-24")).toExternal("20160724");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    StringToLocalDateCodec codec = new StringToLocalDateCodec(format1, UTC, nullStrings);
    assertThat(codec).cannotConvertFromExternal("not a valid date format");
  }
}
