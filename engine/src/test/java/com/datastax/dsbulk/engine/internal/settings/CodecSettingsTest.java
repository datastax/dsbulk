/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.driver.core.DataType.bigint;
import static com.datastax.driver.core.DataType.cboolean;
import static com.datastax.driver.core.DataType.cdouble;
import static com.datastax.driver.core.DataType.cfloat;
import static com.datastax.driver.core.DataType.cint;
import static com.datastax.driver.core.DataType.date;
import static com.datastax.driver.core.DataType.decimal;
import static com.datastax.driver.core.DataType.list;
import static com.datastax.driver.core.DataType.map;
import static com.datastax.driver.core.DataType.set;
import static com.datastax.driver.core.DataType.smallint;
import static com.datastax.driver.core.DataType.time;
import static com.datastax.driver.core.DataType.timestamp;
import static com.datastax.driver.core.DataType.timeuuid;
import static com.datastax.driver.core.DataType.tinyint;
import static com.datastax.driver.core.DataType.uuid;
import static com.datastax.driver.core.DataType.varchar;
import static com.datastax.driver.core.DataType.varint;
import static com.datastax.driver.core.DriverCoreEngineTestHooks.newField;
import static com.datastax.driver.core.DriverCoreEngineTestHooks.newTupleType;
import static com.datastax.driver.core.DriverCoreEngineTestHooks.newUserType;
import static com.datastax.dsbulk.engine.tests.EngineAssertions.assertThat;
import static java.time.ZoneOffset.UTC;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.UserType;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.engine.internal.codecs.ConvertingCodec;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.codecs.json.JsonNodeToUUIDCodec;
import com.datastax.dsbulk.engine.internal.codecs.number.BooleanToNumberCodec;
import com.datastax.dsbulk.engine.internal.codecs.number.NumberToBooleanCodec;
import com.datastax.dsbulk.engine.internal.codecs.number.NumberToNumberCodec;
import com.datastax.dsbulk.engine.internal.codecs.number.NumberToUUIDCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToBigDecimalCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToBigIntegerCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToBooleanCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToByteCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToDoubleCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToFloatCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToInstantCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToIntegerCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToListCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToLocalDateCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToLocalTimeCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToLongCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToMapCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToSetCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToShortCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToTupleCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToUDTCodec;
import com.datastax.dsbulk.engine.internal.codecs.string.StringToUUIDCodec;
import com.datastax.dsbulk.engine.internal.codecs.temporal.DateToTemporalCodec;
import com.datastax.dsbulk.engine.internal.codecs.temporal.DateToUUIDCodec;
import com.datastax.dsbulk.engine.internal.codecs.temporal.TemporalToTemporalCodec;
import com.datastax.dsbulk.engine.internal.codecs.temporal.TemporalToUUIDCodec;
import com.datastax.dsbulk.engine.internal.codecs.util.TimeUUIDGenerator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.google.common.reflect.TypeToken;
import com.typesafe.config.ConfigFactory;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Date;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/** */
class CodecSettingsTest {

  private Cluster cluster;

  @BeforeEach
  void setUp() {
    cluster = mock(Cluster.class);
    Configuration configuration = mock(Configuration.class);
    when(cluster.getConfiguration()).thenReturn(configuration);
    when(configuration.getCodecRegistry()).thenReturn(new CodecRegistry());
  }

  @Test
  void should_return_string_converting_codecs() {

    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.codec"));
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    ExtendedCodecRegistry codecRegistry = settings.createCodecRegistry(cluster);

    assertThat(codecRegistry.codecFor(cboolean(), TypeToken.of(String.class)))
        .isNotNull()
        .isInstanceOf(StringToBooleanCodec.class);
    assertThat(codecRegistry.codecFor(tinyint(), TypeToken.of(String.class)))
        .isNotNull()
        .isInstanceOf(StringToByteCodec.class);
    assertThat(codecRegistry.codecFor(smallint(), TypeToken.of(String.class)))
        .isNotNull()
        .isInstanceOf(StringToShortCodec.class);
    assertThat(codecRegistry.codecFor(cint(), TypeToken.of(String.class)))
        .isNotNull()
        .isInstanceOf(StringToIntegerCodec.class);
    assertThat(codecRegistry.codecFor(bigint(), TypeToken.of(String.class)))
        .isNotNull()
        .isInstanceOf(StringToLongCodec.class);
    assertThat(codecRegistry.codecFor(cfloat(), TypeToken.of(String.class)))
        .isNotNull()
        .isInstanceOf(StringToFloatCodec.class);
    assertThat(codecRegistry.codecFor(cdouble(), TypeToken.of(String.class)))
        .isNotNull()
        .isInstanceOf(StringToDoubleCodec.class);
    assertThat(codecRegistry.codecFor(varint(), TypeToken.of(String.class)))
        .isNotNull()
        .isInstanceOf(StringToBigIntegerCodec.class);
    assertThat(codecRegistry.codecFor(decimal(), TypeToken.of(String.class)))
        .isNotNull()
        .isInstanceOf(StringToBigDecimalCodec.class);
    assertThat(codecRegistry.codecFor(date(), TypeToken.of(String.class)))
        .isNotNull()
        .isInstanceOf(StringToLocalDateCodec.class);
    assertThat(codecRegistry.codecFor(time(), TypeToken.of(String.class)))
        .isNotNull()
        .isInstanceOf(StringToLocalTimeCodec.class);
    assertThat(codecRegistry.codecFor(timestamp(), TypeToken.of(String.class)))
        .isNotNull()
        .isInstanceOf(StringToInstantCodec.class);
    assertThat(codecRegistry.codecFor(uuid(), TypeToken.of(String.class)))
        .isNotNull()
        .isInstanceOf(StringToUUIDCodec.class);
    assertThat(codecRegistry.codecFor(timeuuid(), TypeToken.of(String.class)))
        .isNotNull()
        .isInstanceOf(StringToUUIDCodec.class);
  }

  @Test
  void should_return_number_converting_codecs() {

    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.codec"));
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    ExtendedCodecRegistry codecRegistry = settings.createCodecRegistry(cluster);

    assertThat(codecRegistry.codecFor(tinyint(), TypeToken.of(Short.class)))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(smallint(), TypeToken.of(Integer.class)))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(cint(), TypeToken.of(Long.class)))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(bigint(), TypeToken.of(Float.class)))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(cfloat(), TypeToken.of(Double.class)))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(cdouble(), TypeToken.of(BigDecimal.class)))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(varint(), TypeToken.of(Integer.class)))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(decimal(), TypeToken.of(Float.class)))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
  }

  @Test
  void should_return_temporal_converting_codecs() {

    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.codec"));
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    ExtendedCodecRegistry codecRegistry = settings.createCodecRegistry(cluster);

    assertThat(codecRegistry.convertingCodecFor(date(), TypeToken.of(ZonedDateTime.class)))
        .convertsFrom(ZonedDateTime.parse("2017-11-30T00:00:00+01:00"))
        .to(LocalDate.parse("2017-11-30"))
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.convertingCodecFor(time(), TypeToken.of(ZonedDateTime.class)))
        .convertsFrom(ZonedDateTime.parse("2017-11-30T00:00:00+01:00"))
        .to(LocalTime.parse("00:00:00"))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.convertingCodecFor(timestamp(), TypeToken.of(ZonedDateTime.class)))
        .convertsFrom(ZonedDateTime.parse("2017-11-30T00:00:00+01:00"))
        .to(Instant.parse("2017-11-29T23:00:00Z"))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.convertingCodecFor(date(), TypeToken.of(Instant.class)))
        .convertsFrom(Instant.parse("2017-11-29T23:00:00Z"))
        .to(LocalDate.parse("2017-11-29"))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.convertingCodecFor(time(), TypeToken.of(Instant.class)))
        .convertsFrom(Instant.parse("2017-11-29T23:00:00Z"))
        .to(LocalTime.parse("23:00:00"))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.convertingCodecFor(date(), TypeToken.of(LocalDateTime.class)))
        .convertsFrom(LocalDateTime.parse("2017-11-30T00:00:00"))
        .to(LocalDate.parse("2017-11-30"))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.convertingCodecFor(time(), TypeToken.of(LocalDateTime.class)))
        .convertsFrom(LocalDateTime.parse("2017-11-30T23:00:00"))
        .to(LocalTime.parse("23:00:00"))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.convertingCodecFor(timestamp(), TypeToken.of(LocalDateTime.class)))
        .convertsFrom(LocalDateTime.parse("2017-11-30T00:00:00"))
        .to(Instant.parse("2017-11-30T00:00:00Z"))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.convertingCodecFor(timestamp(), TypeToken.of(LocalDate.class)))
        .convertsFrom(LocalDate.parse("2017-11-30"))
        .to(Instant.parse("2017-11-30T00:00:00Z"))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.convertingCodecFor(timestamp(), TypeToken.of(LocalTime.class)))
        .convertsFrom(LocalTime.parse("23:00:00"))
        .to(Instant.parse("1970-01-01T23:00:00Z"))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.convertingCodecFor(timestamp(), TypeToken.of(java.util.Date.class)))
        .convertsFrom(Date.from(Instant.parse("2017-11-29T23:00:00Z")))
        .to(Instant.parse("2017-11-29T23:00:00Z"))
        .isNotNull()
        .isInstanceOf(DateToTemporalCodec.class);
    assertThat(codecRegistry.convertingCodecFor(date(), TypeToken.of(java.sql.Date.class)))
        .convertsFrom(java.sql.Date.valueOf(LocalDate.parse("2017-11-29")))
        .to(LocalDate.parse("2017-11-29"))
        .isNotNull()
        .isInstanceOf(DateToTemporalCodec.class);
    assertThat(codecRegistry.convertingCodecFor(time(), TypeToken.of(java.sql.Time.class)))
        .convertsFrom(java.sql.Time.valueOf(LocalTime.parse("23:00:00")))
        .to(LocalTime.parse("23:00:00"))
        .isNotNull()
        .isInstanceOf(DateToTemporalCodec.class);
    assertThat(
            codecRegistry.convertingCodecFor(timestamp(), TypeToken.of(java.sql.Timestamp.class)))
        .convertsFrom(Timestamp.from(Instant.parse("2017-11-29T23:00:00Z")))
        .to(Instant.parse("2017-11-29T23:00:00Z"))
        .isNotNull()
        .isInstanceOf(DateToTemporalCodec.class);
  }

  @Test
  void should_return_codecs_for_tokenizable_fields() {

    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.codec"));
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    ExtendedCodecRegistry codecRegistry = settings.createCodecRegistry(cluster);

    assertThat(codecRegistry.codecFor(list(cint()), TypeToken.of(String.class)))
        .isNotNull()
        .isInstanceOf(StringToListCodec.class);
    assertThat(codecRegistry.codecFor(set(cdouble()), TypeToken.of(String.class)))
        .isNotNull()
        .isInstanceOf(StringToSetCodec.class);
    assertThat(codecRegistry.codecFor(map(time(), varchar()), TypeToken.of(String.class)))
        .isNotNull()
        .isInstanceOf(StringToMapCodec.class);
    TupleType tupleType = newTupleType(cint(), cdouble());
    assertThat(codecRegistry.codecFor(tupleType, TypeToken.of(String.class)))
        .isNotNull()
        .isInstanceOf(StringToTupleCodec.class);
    UserType udtType = newUserType(newField("f1", cint()), newField("f2", cdouble()));
    assertThat(codecRegistry.codecFor(udtType, TypeToken.of(String.class)))
        .isNotNull()
        .isInstanceOf(StringToUDTCodec.class);
  }

  @Test
  void should_return_uuid_converting_codecs() {

    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("uuidStrategy = MIN")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.codec")));
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    ExtendedCodecRegistry codecRegistry = settings.createCodecRegistry(cluster);

    assertThat(codecRegistry.convertingCodecFor(timeuuid(), TypeToken.of(Long.class)))
        .isNotNull()
        .isInstanceOf(NumberToUUIDCodec.class)
        .convertsFrom(123456L)
        .to(TimeUUIDGenerator.MIN.generate(Instant.ofEpochMilli(123456L)));
    assertThat(codecRegistry.convertingCodecFor(timeuuid(), TypeToken.of(Instant.class)))
        .isNotNull()
        .isInstanceOf(TemporalToUUIDCodec.class)
        .convertsFrom(Instant.ofEpochMilli(123456L))
        .to(TimeUUIDGenerator.MIN.generate(Instant.ofEpochMilli(123456L)));
    assertThat(codecRegistry.convertingCodecFor(timeuuid(), TypeToken.of(ZonedDateTime.class)))
        .isNotNull()
        .isInstanceOf(TemporalToUUIDCodec.class)
        .convertsFrom(Instant.ofEpochMilli(123456L).atZone(UTC))
        .to(TimeUUIDGenerator.MIN.generate(Instant.ofEpochMilli(123456L)));
    assertThat(codecRegistry.convertingCodecFor(timeuuid(), TypeToken.of(Date.class)))
        .isNotNull()
        .isInstanceOf(DateToUUIDCodec.class)
        .convertsFrom(Date.from(Instant.ofEpochMilli(123456L)))
        .to(TimeUUIDGenerator.MIN.generate(Instant.ofEpochMilli(123456L)));
    assertThat(codecRegistry.convertingCodecFor(timeuuid(), TypeToken.of(java.sql.Timestamp.class)))
        .isNotNull()
        .isInstanceOf(DateToUUIDCodec.class)
        .convertsFrom(Timestamp.from(Instant.ofEpochMilli(123456L)))
        .to(TimeUUIDGenerator.MIN.generate(Instant.ofEpochMilli(123456L)));
    assertThat(codecRegistry.convertingCodecFor(timeuuid(), TypeToken.of(String.class)))
        .isNotNull()
        .isInstanceOf(StringToUUIDCodec.class)
        .convertsFrom("123456")
        .to(TimeUUIDGenerator.MIN.generate(Instant.ofEpochMilli(123456L)));
    assertThat(codecRegistry.convertingCodecFor(timeuuid(), TypeToken.of(JsonNode.class)))
        .isNotNull()
        .isInstanceOf(JsonNodeToUUIDCodec.class)
        .convertsFrom(JsonNodeFactory.instance.textNode("123456"))
        .to(TimeUUIDGenerator.MIN.generate(Instant.ofEpochMilli(123456L)));
  }

  @Test
  void should_return_boolean_converting_codecs() {

    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.codec"));
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    ExtendedCodecRegistry codecRegistry = settings.createCodecRegistry(cluster);

    assertThat(codecRegistry.convertingCodecFor(tinyint(), TypeToken.of(Boolean.class)))
        .isNotNull()
        .isInstanceOf(BooleanToNumberCodec.class)
        .convertsFrom(true)
        .to((byte) 1);
    assertThat(codecRegistry.convertingCodecFor(cboolean(), TypeToken.of(Byte.class)))
        .isNotNull()
        .isInstanceOf(NumberToBooleanCodec.class)
        .convertsFrom((byte) 1)
        .to(true);
  }

  @Test
  void should_return_rounding_codecs() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("roundingStrategy = UP")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.codec")));
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    ExtendedCodecRegistry codecRegistry = settings.createCodecRegistry(cluster);
    ConvertingCodec<String, Float> codec =
        codecRegistry.convertingCodecFor(cfloat(), TypeToken.of(String.class));
    assertThat(codec.convertTo(0.123f)).isEqualTo("0.13");
  }

  @Test
  void should_return_codecs_honoring_overflow_strategy() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("overflowStrategy = TRUNCATE")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.codec")));
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    ExtendedCodecRegistry codecRegistry = settings.createCodecRegistry(cluster);
    ConvertingCodec<String, Byte> codec =
        codecRegistry.convertingCodecFor(tinyint(), TypeToken.of(String.class));
    assertThat(codec.convertFrom("128")).isEqualTo((byte) 127);
  }
}
