/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.oss.dsbulk.codecs.text.json;

import static com.datastax.oss.dsbulk.codecs.text.TextConversionContext.JSON_NODE_TYPE;
import static com.datastax.oss.dsbulk.codecs.text.json.JsonCodecUtils.JSON_NODE_FACTORY;
import static com.datastax.oss.dsbulk.tests.assertions.TestAssertions.assertThat;
import static java.util.concurrent.TimeUnit.MINUTES;

import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.dsbulk.codecs.ConversionContext;
import com.datastax.oss.dsbulk.codecs.ConvertingCodecFactory;
import com.datastax.oss.dsbulk.codecs.text.TextConversionContext;
import com.datastax.oss.dsbulk.codecs.util.CqlTemporalFormat;
import com.fasterxml.jackson.databind.JsonNode;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class JsonNodeToInstantCodecTest {

  private final Instant millennium = Instant.parse("2000-01-01T00:00:00Z");
  private final Instant minutesAfterMillennium = millennium.plus(Duration.ofMinutes(123456));

  private JsonNodeToInstantCodec codec1;
  private JsonNodeToInstantCodec codec2;
  private JsonNodeToInstantCodec codec3;
  private JsonNodeToInstantCodec codec4;

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
        (JsonNodeToInstantCodec)
            new ConvertingCodecFactory(context1)
                .<JsonNode, Instant>createConvertingCodec(
                    DataTypes.TIMESTAMP, JSON_NODE_TYPE, true);
    codec2 =
        (JsonNodeToInstantCodec)
            new ConvertingCodecFactory(context2)
                .<JsonNode, Instant>createConvertingCodec(
                    DataTypes.TIMESTAMP, JSON_NODE_TYPE, true);
    codec3 =
        (JsonNodeToInstantCodec)
            new ConvertingCodecFactory(context3)
                .<JsonNode, Instant>createConvertingCodec(
                    DataTypes.TIMESTAMP, JSON_NODE_TYPE, true);
    codec4 =
        (JsonNodeToInstantCodec)
            new ConvertingCodecFactory(context4)
                .<JsonNode, Instant>createConvertingCodec(
                    DataTypes.TIMESTAMP, JSON_NODE_TYPE, true);
  }

  @Test
  void should_convert_from_valid_external() {
    assertThat(codec1)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("2016-07-24T20:34"))
        .toInternal(Instant.parse("2016-07-24T20:34:00Z"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("2016-07-24T20:34:12"))
        .toInternal(Instant.parse("2016-07-24T20:34:12Z"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("2016-07-24T20:34:12.999"))
        .toInternal(Instant.parse("2016-07-24T20:34:12.999Z"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("2016-07-24T20:34+01:00"))
        .toInternal(Instant.parse("2016-07-24T19:34:00Z"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("2016-07-24T20:34:12.999+01:00"))
        .toInternal(Instant.parse("2016-07-24T19:34:12.999Z"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);
    assertThat(codec2)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("20160724203412"))
        .toInternal(Instant.parse("2016-07-24T20:34:12Z"))
        .convertsFromExternal(JSON_NODE_FACTORY.textNode(""))
        .toInternal(null)
        .convertsFromExternal(null)
        .toInternal(null)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("NULL"))
        .toInternal(null);
    assertThat(codec3)
        .convertsFromExternal(
            JSON_NODE_FACTORY.textNode(
                CqlTemporalFormat.DEFAULT_INSTANCE.format(minutesAfterMillennium)))
        .toInternal(minutesAfterMillennium);
    assertThat(codec4)
        .convertsFromExternal(JSON_NODE_FACTORY.numberNode(123456))
        .toInternal(minutesAfterMillennium)
        .convertsFromExternal(JSON_NODE_FACTORY.textNode("123456"))
        .toInternal(minutesAfterMillennium);
  }

  @Test
  void should_convert_from_valid_internal() {
    assertThat(codec1)
        .convertsFromInternal(Instant.parse("2016-07-24T20:34:00Z"))
        .toExternal(JSON_NODE_FACTORY.textNode("2016-07-24T20:34:00Z"))
        .convertsFromInternal(Instant.parse("2016-07-24T20:34:12Z"))
        .toExternal(JSON_NODE_FACTORY.textNode("2016-07-24T20:34:12Z"))
        .convertsFromInternal(Instant.parse("2016-07-24T20:34:12.999Z"))
        .toExternal(JSON_NODE_FACTORY.textNode("2016-07-24T20:34:12.999Z"))
        .convertsFromInternal(Instant.parse("2016-07-24T19:34:00.000Z"))
        .toExternal(JSON_NODE_FACTORY.textNode("2016-07-24T19:34:00Z"))
        .convertsFromInternal(Instant.parse("2016-07-24T19:34:12.999Z"))
        .toExternal(JSON_NODE_FACTORY.textNode("2016-07-24T19:34:12.999Z"))
        .convertsFromInternal(null)
        .toExternal(null);
    assertThat(codec2)
        .convertsFromInternal(Instant.parse("2016-07-24T20:34:12Z"))
        .toExternal(JSON_NODE_FACTORY.textNode("20160724203412"))
        .convertsFromInternal(null)
        .toExternal(null);
    // conversion back to numeric timestamps is not possible, values are always formatted with full
    // alphanumeric pattern
    assertThat(codec3)
        .convertsFromInternal(minutesAfterMillennium)
        .toExternal(
            JSON_NODE_FACTORY.textNode(
                CqlTemporalFormat.DEFAULT_INSTANCE.format(minutesAfterMillennium)));
    assertThat(codec4)
        .convertsFromInternal(minutesAfterMillennium)
        .toExternal(JSON_NODE_FACTORY.numberNode(123456L));
  }

  @Test
  void should_not_convert_from_invalid_external() {
    assertThat(codec1)
        .cannotConvertFromExternal(JSON_NODE_FACTORY.textNode("not a valid date format"));
  }
}
