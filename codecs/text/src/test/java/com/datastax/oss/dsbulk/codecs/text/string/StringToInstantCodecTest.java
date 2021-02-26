/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.dsbulk.codecs.text.string;

import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.dsbulk.codecs.api.ConversionContext;
import com.datastax.oss.dsbulk.codecs.api.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.api.format.temporal.CqlTemporalFormat;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
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
    ConversionContext context1 = new TextConversionContext().setNullStrings("NULL");
    ConversionContext context2 =
        new TextConversionContext().setNullStrings("NULL").setTimestampFormat("yyyyMMddHHmmss");
    ConversionContext context3 =
        new TextConversionContext()
            .setNullStrings("NULL")
            .setTimeUnit(MINUTES)
            .setEpoch(ZonedDateTime.parse("2000-01-01T00:00:00Z"));
    ConversionContext context4 =
        new TextConversionContext()
            .setNullStrings("NULL")
            .setTimeUnit(MINUTES)
            .setEpoch(ZonedDateTime.parse("2000-01-01T00:00:00Z"))
            .setTimestampFormat("UNITS_SINCE_EPOCH");
    codec1 =
        (StringToInstantCodec)
            new ConvertingCodecFactory(context1)
                .<String, Instant>createConvertingCodec(
                    DataTypes.TIMESTAMP, GenericType.STRING, true);
    codec2 =
        (StringToInstantCodec)
            new ConvertingCodecFactory(context2)
                .<String, Instant>createConvertingCodec(
                    DataTypes.TIMESTAMP, GenericType.STRING, true);
    codec3 =
        (StringToInstantCodec)
            new ConvertingCodecFactory(context3)
                .<String, Instant>createConvertingCodec(
                    DataTypes.TIMESTAMP, GenericType.STRING, true);
    codec4 =
        (StringToInstantCodec)
            new ConvertingCodecFactory(context4)
                .<String, Instant>createConvertingCodec(
                    DataTypes.TIMESTAMP, GenericType.STRING, true);
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
