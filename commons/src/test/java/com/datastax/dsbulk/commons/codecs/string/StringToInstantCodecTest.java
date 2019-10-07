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
import static java.util.concurrent.TimeUnit.MINUTES;

import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistryBuilder;
import com.datastax.dsbulk.commons.codecs.util.CqlTemporalFormat;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class StringToInstantCodecTest {

  private final Instant millennium = Instant.parse("2000-01-01T00:00:00Z");
  private final Instant minutesAfterMillennium = millennium.plus(Duration.ofMinutes(123456));

  private StringToInstantCodec codec1;
  private StringToInstantCodec codec2;
  private StringToInstantCodec codec3;
  private StringToInstantCodec codec4;

  @BeforeEach
  void setUpCodec1() {
    codec1 =
        (StringToInstantCodec)
            new ExtendedCodecRegistryBuilder()
                .withNullStrings("NULL")
                .build()
                .codecFor(DataTypes.TIMESTAMP, GenericType.STRING);
    codec2 =
        (StringToInstantCodec)
            new ExtendedCodecRegistryBuilder()
                .withNullStrings("NULL")
                .withTimestampFormat("yyyyMMddHHmmss")
                .build()
                .codecFor(DataTypes.TIMESTAMP, GenericType.STRING);
    codec3 =
        (StringToInstantCodec)
            new ExtendedCodecRegistryBuilder()
                .withNullStrings("NULL")
                .withTimeUnit(MINUTES)
                .withEpoch(ZonedDateTime.parse("2000-01-01T00:00:00Z"))
                .build()
                .codecFor(DataTypes.TIMESTAMP, GenericType.STRING);
    codec4 =
        (StringToInstantCodec)
            new ExtendedCodecRegistryBuilder()
                .withNullStrings("NULL")
                .withTimeUnit(MINUTES)
                .withEpoch(ZonedDateTime.parse("2000-01-01T00:00:00Z"))
                .withTimestampFormat("UNITS_SINCE_EPOCH")
                .build()
                .codecFor(DataTypes.TIMESTAMP, GenericType.STRING);
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec1)
        .convertsFromExternal("2016-07-24T20:34")
        .toInternal(Instant.parse("2016-07-24T20:34:00Z"))
        .convertsFromExternal("2016-07-24T20:34:12")
        .toInternal(Instant.parse("2016-07-24T20:34:12Z"))
        .convertsFromExternal("2016-07-24T20:34:12.999")
        .toInternal(Instant.parse("2016-07-24T20:34:12.999Z"))
        .convertsFromExternal("2016-07-24T20:34+01:00")
        .toInternal(Instant.parse("2016-07-24T19:34:00Z"))
        .convertsFromExternal("2016-07-24T20:34:12.999+01:00")
        .toInternal(Instant.parse("2016-07-24T19:34:12.999Z"))
        .convertsFromExternal("")
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal("NULL")
        .toInternal(null);
    assertThat(codec2)
        .convertsFromExternal("20160724203412")
        .toInternal(Instant.parse("2016-07-24T20:34:12Z"))
        .convertsFromExternal("NULL")
        .toInternal(null);
    assertThat(codec3)
        .convertsFromExternal(CqlTemporalFormat.DEFAULT_INSTANCE.format(minutesAfterMillennium))
        .toInternal(minutesAfterMillennium);
    assertThat(codec4).convertsFromExternal("123456").toInternal(minutesAfterMillennium);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec1)
        .convertsFromInternal(Instant.parse("2016-07-24T20:34:00Z"))
        .toExternal("2016-07-24T20:34:00Z")
        .convertsFromInternal(Instant.parse("2016-07-24T20:34:12Z"))
        .toExternal("2016-07-24T20:34:12Z")
        .convertsFromInternal(Instant.parse("2016-07-24T20:34:12.999Z"))
        .toExternal("2016-07-24T20:34:12.999Z")
        .convertsFromInternal(Instant.parse("2016-07-24T19:34:00Z"))
        .toExternal("2016-07-24T19:34:00Z")
        .convertsFromInternal(Instant.parse("2016-07-24T19:34:12.999Z"))
        .toExternal("2016-07-24T19:34:12.999Z")
        .convertsFromInternal(null)
        .toExternal("NULL");
    assertThat(codec2)
        .convertsFromInternal(Instant.parse("2016-07-24T20:34:12Z"))
        .toExternal("20160724203412")
        .convertsFromInternal(null)
        .toExternal("NULL");
    // conversion back to numeric timestamps is not possible, values are always formatted with full
    // alphanumeric pattern
    assertThat(codec3)
        .convertsFromInternal(minutesAfterMillennium)
        .toExternal(CqlTemporalFormat.DEFAULT_INSTANCE.format(minutesAfterMillennium));
    assertThat(codec4).convertsFromInternal(minutesAfterMillennium).toExternal("123456");
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec1).cannotConvertFromExternal("not a valid date format");
  }
}
