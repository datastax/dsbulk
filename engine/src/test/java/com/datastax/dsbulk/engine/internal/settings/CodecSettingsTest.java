/*
 * Copyright DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
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
import static com.datastax.dsbulk.engine.internal.EngineAssertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.UserType;
import com.datastax.dsbulk.commons.config.LoaderConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultLoaderConfig;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.codecs.number.NumberToNumberCodec;
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
import com.datastax.dsbulk.engine.internal.codecs.temporal.TemporalToTemporalCodec;
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
        .to(LocalDate.parse("2017-11-29"))
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.convertingCodecFor(time(), TypeToken.of(ZonedDateTime.class)))
        .convertsFrom(ZonedDateTime.parse("2017-11-30T00:00:00+01:00"))
        .to(LocalTime.parse("23:00:00"))
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
}
