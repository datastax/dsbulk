/*
 * Copyright DataStax, Inc.
 *
 * This software is subject to the below license agreement.
 * DataStax may make changes to the agreement from time to time,
 * and will post the amended terms at
 * https://www.datastax.com/terms/datastax-dse-bulk-utility-license-terms.
 */
package com.datastax.dsbulk.engine.internal.settings;

import static com.datastax.dsbulk.commons.tests.assertions.CommonsAssertions.assertThat;
import static java.time.ZoneOffset.UTC;

import com.datastax.dsbulk.commons.codecs.ConvertingCodec;
import com.datastax.dsbulk.commons.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.commons.codecs.json.JsonNodeToUnknownTypeCodec;
import com.datastax.dsbulk.commons.codecs.number.BooleanToNumberCodec;
import com.datastax.dsbulk.commons.codecs.number.NumberToBooleanCodec;
import com.datastax.dsbulk.commons.codecs.number.NumberToNumberCodec;
import com.datastax.dsbulk.commons.codecs.number.NumberToUUIDCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToBigDecimalCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToBigIntegerCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToBooleanCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToByteCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToDoubleCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToFloatCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToInstantCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToIntegerCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToListCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToLocalDateCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToLocalTimeCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToLongCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToMapCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToSetCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToShortCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToTupleCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToUDTCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToUUIDCodec;
import com.datastax.dsbulk.commons.codecs.string.StringToUnknownTypeCodec;
import com.datastax.dsbulk.commons.codecs.temporal.DateToTemporalCodec;
import com.datastax.dsbulk.commons.codecs.temporal.DateToUUIDCodec;
import com.datastax.dsbulk.commons.codecs.temporal.TemporalToTemporalCodec;
import com.datastax.dsbulk.commons.codecs.temporal.TemporalToUUIDCodec;
import com.datastax.dsbulk.commons.codecs.util.TimeUUIDGenerator;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.commons.tests.driver.DriverUtils;
import com.datastax.dsbulk.commons.tests.utils.ReflectionUtils;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.TupleType;
import com.datastax.oss.driver.api.core.type.UserDefinedType;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.internal.core.type.UserDefinedTypeBuilder;
import com.fasterxml.jackson.databind.JsonNode;
import com.typesafe.config.ConfigFactory;
import edu.umd.cs.findbugs.annotations.NonNull;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import java.util.Date;
import java.util.List;
import org.junit.jupiter.api.Test;

class CodecSettingsTest {

  @Test
  void should_return_string_converting_codecs() {

    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.codec"));
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    ExtendedCodecRegistry codecRegistry = settings.createCodecRegistry(false, false);

    assertThat(codecRegistry.codecFor(DataTypes.BOOLEAN, GenericType.STRING))
        .isNotNull()
        .isInstanceOf(StringToBooleanCodec.class);
    assertThat(codecRegistry.codecFor(DataTypes.TINYINT, GenericType.STRING))
        .isNotNull()
        .isInstanceOf(StringToByteCodec.class);
    assertThat(codecRegistry.codecFor(DataTypes.SMALLINT, GenericType.STRING))
        .isNotNull()
        .isInstanceOf(StringToShortCodec.class);
    assertThat(codecRegistry.codecFor(DataTypes.INT, GenericType.STRING))
        .isNotNull()
        .isInstanceOf(StringToIntegerCodec.class);
    assertThat(codecRegistry.codecFor(DataTypes.BIGINT, GenericType.STRING))
        .isNotNull()
        .isInstanceOf(StringToLongCodec.class);
    assertThat(codecRegistry.codecFor(DataTypes.FLOAT, GenericType.STRING))
        .isNotNull()
        .isInstanceOf(StringToFloatCodec.class);
    assertThat(codecRegistry.codecFor(DataTypes.DOUBLE, GenericType.STRING))
        .isNotNull()
        .isInstanceOf(StringToDoubleCodec.class);
    assertThat(codecRegistry.codecFor(DataTypes.VARINT, GenericType.STRING))
        .isNotNull()
        .isInstanceOf(StringToBigIntegerCodec.class);
    assertThat(codecRegistry.codecFor(DataTypes.DECIMAL, GenericType.STRING))
        .isNotNull()
        .isInstanceOf(StringToBigDecimalCodec.class);
    assertThat(codecRegistry.codecFor(DataTypes.DATE, GenericType.STRING))
        .isNotNull()
        .isInstanceOf(StringToLocalDateCodec.class);
    assertThat(codecRegistry.codecFor(DataTypes.TIME, GenericType.STRING))
        .isNotNull()
        .isInstanceOf(StringToLocalTimeCodec.class);
    assertThat(codecRegistry.codecFor(DataTypes.TIMESTAMP, GenericType.STRING))
        .isNotNull()
        .isInstanceOf(StringToInstantCodec.class);
    assertThat(codecRegistry.codecFor(DataTypes.UUID, GenericType.STRING))
        .isNotNull()
        .isInstanceOf(StringToUUIDCodec.class);
    assertThat(codecRegistry.codecFor(DataTypes.TIMEUUID, GenericType.STRING))
        .isNotNull()
        .isInstanceOf(StringToUUIDCodec.class);
  }

  @Test
  void should_return_number_converting_codecs() {

    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.codec"));
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    ExtendedCodecRegistry codecRegistry = settings.createCodecRegistry(false, false);

    assertThat(codecRegistry.codecFor(DataTypes.TINYINT, GenericType.SHORT))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(DataTypes.SMALLINT, GenericType.INTEGER))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(DataTypes.INT, GenericType.LONG))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(DataTypes.BIGINT, GenericType.FLOAT))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(DataTypes.FLOAT, GenericType.DOUBLE))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(DataTypes.DOUBLE, GenericType.BIG_DECIMAL))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(DataTypes.VARINT, GenericType.INTEGER))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(DataTypes.DECIMAL, GenericType.FLOAT))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
  }

  @Test
  void should_return_temporal_converting_codecs() {

    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.codec"));
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    ExtendedCodecRegistry codecRegistry = settings.createCodecRegistry(false, false);

    assertThat(
            codecRegistry.convertingCodecFor(DataTypes.DATE, GenericType.of(ZonedDateTime.class)))
        .convertsFromExternal(ZonedDateTime.parse("2017-11-30T00:00:00+01:00"))
        .toInternal(LocalDate.parse("2017-11-30"))
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(
            codecRegistry.convertingCodecFor(DataTypes.TIME, GenericType.of(ZonedDateTime.class)))
        .convertsFromExternal(ZonedDateTime.parse("2017-11-30T00:00:00+01:00"))
        .toInternal(LocalTime.parse("00:00:00"))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(
            codecRegistry.convertingCodecFor(
                DataTypes.TIMESTAMP, GenericType.of(ZonedDateTime.class)))
        .convertsFromExternal(ZonedDateTime.parse("2017-11-30T00:00:00+01:00"))
        .toInternal(Instant.parse("2017-11-29T23:00:00Z"))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.convertingCodecFor(DataTypes.DATE, GenericType.INSTANT))
        .convertsFromExternal(Instant.parse("2017-11-29T23:00:00Z"))
        .toInternal(LocalDate.parse("2017-11-29"))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.convertingCodecFor(DataTypes.TIME, GenericType.INSTANT))
        .convertsFromExternal(Instant.parse("2017-11-29T23:00:00Z"))
        .toInternal(LocalTime.parse("23:00:00"))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(
            codecRegistry.convertingCodecFor(DataTypes.DATE, GenericType.of(LocalDateTime.class)))
        .convertsFromExternal(LocalDateTime.parse("2017-11-30T00:00:00"))
        .toInternal(LocalDate.parse("2017-11-30"))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(
            codecRegistry.convertingCodecFor(DataTypes.TIME, GenericType.of(LocalDateTime.class)))
        .convertsFromExternal(LocalDateTime.parse("2017-11-30T23:00:00"))
        .toInternal(LocalTime.parse("23:00:00"))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(
            codecRegistry.convertingCodecFor(
                DataTypes.TIMESTAMP, GenericType.of(LocalDateTime.class)))
        .convertsFromExternal(LocalDateTime.parse("2017-11-30T00:00:00"))
        .toInternal(Instant.parse("2017-11-30T00:00:00Z"))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.convertingCodecFor(DataTypes.TIMESTAMP, GenericType.LOCAL_DATE))
        .convertsFromExternal(LocalDate.parse("2017-11-30"))
        .toInternal(Instant.parse("2017-11-30T00:00:00Z"))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.convertingCodecFor(DataTypes.TIMESTAMP, GenericType.LOCAL_TIME))
        .convertsFromExternal(LocalTime.parse("23:00:00"))
        .toInternal(Instant.parse("1970-01-01T23:00:00Z"))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(
            codecRegistry.convertingCodecFor(
                DataTypes.TIMESTAMP, GenericType.of(java.util.Date.class)))
        .convertsFromExternal(Date.from(Instant.parse("2017-11-29T23:00:00Z")))
        .toInternal(Instant.parse("2017-11-29T23:00:00Z"))
        .isNotNull()
        .isInstanceOf(DateToTemporalCodec.class);
    assertThat(
            codecRegistry.convertingCodecFor(DataTypes.DATE, GenericType.of(java.sql.Date.class)))
        .convertsFromExternal(java.sql.Date.valueOf(LocalDate.parse("2017-11-29")))
        .toInternal(LocalDate.parse("2017-11-29"))
        .isNotNull()
        .isInstanceOf(DateToTemporalCodec.class);
    assertThat(
            codecRegistry.convertingCodecFor(DataTypes.TIME, GenericType.of(java.sql.Time.class)))
        .convertsFromExternal(java.sql.Time.valueOf(LocalTime.parse("23:00:00")))
        .toInternal(LocalTime.parse("23:00:00"))
        .isNotNull()
        .isInstanceOf(DateToTemporalCodec.class);
    assertThat(
            codecRegistry.convertingCodecFor(
                DataTypes.TIMESTAMP, GenericType.of(java.sql.Timestamp.class)))
        .convertsFromExternal(Timestamp.from(Instant.parse("2017-11-29T23:00:00Z")))
        .toInternal(Instant.parse("2017-11-29T23:00:00Z"))
        .isNotNull()
        .isInstanceOf(DateToTemporalCodec.class);
  }

  @Test
  void should_return_codecs_for_tokenizable_fields() {

    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.codec"));
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    ExtendedCodecRegistry codecRegistry = settings.createCodecRegistry(false, false);

    assertThat(codecRegistry.codecFor(DataTypes.listOf(DataTypes.INT), GenericType.STRING))
        .isNotNull()
        .isInstanceOf(StringToListCodec.class);
    assertThat(codecRegistry.codecFor(DataTypes.setOf(DataTypes.DOUBLE), GenericType.STRING))
        .isNotNull()
        .isInstanceOf(StringToSetCodec.class);
    assertThat(
            codecRegistry.codecFor(
                DataTypes.mapOf(DataTypes.TIME, DataTypes.TEXT), GenericType.STRING))
        .isNotNull()
        .isInstanceOf(StringToMapCodec.class);
    TupleType tupleType = DriverUtils.mockTupleType(DataTypes.INT, DataTypes.DOUBLE);
    assertThat(codecRegistry.codecFor(tupleType, GenericType.STRING))
        .isNotNull()
        .isInstanceOf(StringToTupleCodec.class);
    UserDefinedType udtType =
        new UserDefinedTypeBuilder("ks", "udt")
            .withField("f1", DataTypes.INT)
            .withField("f2", DataTypes.DOUBLE)
            .build();
    assertThat(codecRegistry.codecFor(udtType, GenericType.STRING))
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
    ExtendedCodecRegistry codecRegistry = settings.createCodecRegistry(false, false);

    assertThat(codecRegistry.convertingCodecFor(DataTypes.TIMEUUID, GenericType.LONG))
        .isNotNull()
        .isInstanceOf(NumberToUUIDCodec.class)
        .convertsFromExternal(123456L)
        .toInternal(TimeUUIDGenerator.MIN.generate(Instant.ofEpochMilli(123456L)));
    assertThat(codecRegistry.convertingCodecFor(DataTypes.TIMEUUID, GenericType.INSTANT))
        .isNotNull()
        .isInstanceOf(TemporalToUUIDCodec.class)
        .convertsFromExternal(Instant.ofEpochMilli(123456L))
        .toInternal(TimeUUIDGenerator.MIN.generate(Instant.ofEpochMilli(123456L)));
    assertThat(
            codecRegistry.convertingCodecFor(
                DataTypes.TIMEUUID, GenericType.of(ZonedDateTime.class)))
        .isNotNull()
        .isInstanceOf(TemporalToUUIDCodec.class)
        .convertsFromExternal(Instant.ofEpochMilli(123456L).atZone(UTC))
        .toInternal(TimeUUIDGenerator.MIN.generate(Instant.ofEpochMilli(123456L)));
    assertThat(codecRegistry.convertingCodecFor(DataTypes.TIMEUUID, GenericType.of(Date.class)))
        .isNotNull()
        .isInstanceOf(DateToUUIDCodec.class)
        .convertsFromExternal(Date.from(Instant.ofEpochMilli(123456L)))
        .toInternal(TimeUUIDGenerator.MIN.generate(Instant.ofEpochMilli(123456L)));
    assertThat(
            codecRegistry.convertingCodecFor(
                DataTypes.TIMEUUID, GenericType.of(java.sql.Timestamp.class)))
        .isNotNull()
        .isInstanceOf(DateToUUIDCodec.class)
        .convertsFromExternal(Timestamp.from(Instant.ofEpochMilli(123456L)))
        .toInternal(TimeUUIDGenerator.MIN.generate(Instant.ofEpochMilli(123456L)));
  }

  @Test
  void should_return_boolean_converting_codecs() {
    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.codec"));
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    ExtendedCodecRegistry codecRegistry = settings.createCodecRegistry(false, false);

    assertThat(codecRegistry.convertingCodecFor(DataTypes.TINYINT, GenericType.BOOLEAN))
        .isNotNull()
        .isInstanceOf(BooleanToNumberCodec.class)
        .convertsFromExternal(true)
        .toInternal((byte) 1);
    assertThat(codecRegistry.convertingCodecFor(DataTypes.BOOLEAN, GenericType.BYTE))
        .isNotNull()
        .isInstanceOf(NumberToBooleanCodec.class)
        .convertsFromExternal((byte) 1)
        .toInternal(true);
  }

  @Test
  void should_return_rounding_codecs() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("roundingStrategy = UP, formatNumbers = true")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.codec")));
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    ExtendedCodecRegistry codecRegistry = settings.createCodecRegistry(false, false);
    ConvertingCodec<String, Float> codec =
        codecRegistry.convertingCodecFor(DataTypes.FLOAT, GenericType.STRING);
    assertThat(codec.internalToExternal(0.123f)).isEqualTo("0.13");
  }

  @Test
  void should_return_codecs_honoring_overflow_strategy() {
    LoaderConfig config =
        new DefaultLoaderConfig(
            ConfigFactory.parseString("overflowStrategy = TRUNCATE")
                .withFallback(ConfigFactory.load().getConfig("dsbulk.codec")));
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    ExtendedCodecRegistry codecRegistry = settings.createCodecRegistry(false, false);
    ConvertingCodec<String, Byte> codec =
        codecRegistry.convertingCodecFor(DataTypes.TINYINT, GenericType.STRING);
    assertThat(codec.externalToInternal("128")).isEqualTo((byte) 127);
  }

  @SuppressWarnings("unchecked")
  @Test
  void should_create_settings_when_null_words_are_specified() {
    {
      LoaderConfig config = makeLoaderConfig("nullStrings = [NULL]");
      CodecSettings codecSettings = new CodecSettings(config);
      codecSettings.init();

      assertThat((List<String>) ReflectionUtils.getInternalState(codecSettings, "nullStrings"))
          .containsOnly("NULL");
    }
    {
      LoaderConfig config = makeLoaderConfig("nullStrings = [NIL, NULL]");
      CodecSettings codecSettings = new CodecSettings(config);
      codecSettings.init();

      assertThat((List<String>) ReflectionUtils.getInternalState(codecSettings, "nullStrings"))
          .containsOnly("NIL", "NULL");
    }
    {
      LoaderConfig config = makeLoaderConfig("nullStrings = [\"NULL\"]");
      CodecSettings codecSettings = new CodecSettings(config);
      codecSettings.init();

      assertThat((List<String>) ReflectionUtils.getInternalState(codecSettings, "nullStrings"))
          .containsOnly("NULL");
    }
    {
      LoaderConfig config = makeLoaderConfig("nullStrings = [\"NIL\", \"NULL\"]");
      CodecSettings codecSettings = new CodecSettings(config);
      codecSettings.init();

      assertThat((List<String>) ReflectionUtils.getInternalState(codecSettings, "nullStrings"))
          .containsOnly("NIL", "NULL");
    }
  }

  @Test
  void should_return_string_to_unknown_type_codec_for_dynamic_composite_type() {
    LoaderConfig config = new DefaultLoaderConfig(ConfigFactory.load().getConfig("dsbulk.codec"));
    CodecSettings settings = new CodecSettings(config);
    settings.init();
    ExtendedCodecRegistry codecRegistry = settings.createCodecRegistry(false, false);
    assertThat(
            codecRegistry.convertingCodecFor(
                DataTypes.custom("org.apache.cassandra.db.marshal.DynamicCompositeType"),
                GenericType.STRING))
        .isNotNull()
        .isInstanceOf(StringToUnknownTypeCodec.class);
    assertThat(
            codecRegistry.convertingCodecFor(
                DataTypes.custom("org.apache.cassandra.db.marshal.DynamicCompositeType"),
                GenericType.of(JsonNode.class)))
        .isNotNull()
        .isInstanceOf(JsonNodeToUnknownTypeCodec.class);
  }

  @NonNull
  private static LoaderConfig makeLoaderConfig(String configString) {
    return new DefaultLoaderConfig(
        ConfigFactory.parseString(configString)
            .withFallback(ConfigFactory.load().getConfig("dsbulk.codec")));
  }
}
