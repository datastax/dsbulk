/*
 * Copyright (C) 2017 DataStax Inc.
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
import static com.datastax.driver.core.DriverCoreTestHooks.newField;
import static com.datastax.driver.core.DriverCoreTestHooks.newTupleType;
import static com.datastax.driver.core.DriverCoreTestHooks.newUserType;
import static com.datastax.dsbulk.engine.internal.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.UserType;
import com.datastax.dsbulk.commons.config.DSBulkConfig;
import com.datastax.dsbulk.commons.internal.config.DefaultDSBulkConfig;
import com.datastax.dsbulk.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.dsbulk.engine.internal.codecs.NumberToNumberCodec;
import com.datastax.dsbulk.engine.internal.codecs.StringToBigDecimalCodec;
import com.datastax.dsbulk.engine.internal.codecs.StringToBigIntegerCodec;
import com.datastax.dsbulk.engine.internal.codecs.StringToBooleanCodec;
import com.datastax.dsbulk.engine.internal.codecs.StringToByteCodec;
import com.datastax.dsbulk.engine.internal.codecs.StringToDoubleCodec;
import com.datastax.dsbulk.engine.internal.codecs.StringToFloatCodec;
import com.datastax.dsbulk.engine.internal.codecs.StringToInstantCodec;
import com.datastax.dsbulk.engine.internal.codecs.StringToIntegerCodec;
import com.datastax.dsbulk.engine.internal.codecs.StringToListCodec;
import com.datastax.dsbulk.engine.internal.codecs.StringToLocalDateCodec;
import com.datastax.dsbulk.engine.internal.codecs.StringToLocalTimeCodec;
import com.datastax.dsbulk.engine.internal.codecs.StringToLongCodec;
import com.datastax.dsbulk.engine.internal.codecs.StringToMapCodec;
import com.datastax.dsbulk.engine.internal.codecs.StringToSetCodec;
import com.datastax.dsbulk.engine.internal.codecs.StringToShortCodec;
import com.datastax.dsbulk.engine.internal.codecs.StringToTupleCodec;
import com.datastax.dsbulk.engine.internal.codecs.StringToUDTCodec;
import com.datastax.dsbulk.engine.internal.codecs.StringToUUIDCodec;
import com.datastax.dsbulk.engine.internal.codecs.TemporalToTemporalCodec;
import com.google.common.reflect.TypeToken;
import com.typesafe.config.ConfigFactory;
import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZonedDateTime;
import org.junit.Before;
import org.junit.Test;

/** */
public class CodecSettingsTest {

  private Cluster cluster;

  @Before
  public void setUp() throws Exception {
    cluster = mock(Cluster.class);
    Configuration configuration = mock(Configuration.class);
    when(cluster.getConfiguration()).thenReturn(configuration);
    when(configuration.getCodecRegistry()).thenReturn(new CodecRegistry());
  }

  @Test
  public void should_return_string_converting_codecs() throws Exception {

    DSBulkConfig config = new DefaultDSBulkConfig(ConfigFactory.load().getConfig("dsbulk.codec"));
    CodecSettings settings = new CodecSettings(config);
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
  public void should_return_number_converting_codecs() throws Exception {

    DSBulkConfig config = new DefaultDSBulkConfig(ConfigFactory.load().getConfig("dsbulk.codec"));
    CodecSettings settings = new CodecSettings(config);
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
  public void should_return_temporal_converting_codecs() throws Exception {

    DSBulkConfig config = new DefaultDSBulkConfig(ConfigFactory.load().getConfig("dsbulk.codec"));
    CodecSettings settings = new CodecSettings(config);
    ExtendedCodecRegistry codecRegistry = settings.createCodecRegistry(cluster);

    assertThat(codecRegistry.codecFor(date(), TypeToken.of(ZonedDateTime.class)))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(time(), TypeToken.of(ZonedDateTime.class)))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(timestamp(), TypeToken.of(ZonedDateTime.class)))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(date(), TypeToken.of(Instant.class)))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(time(), TypeToken.of(Instant.class)))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(date(), TypeToken.of(LocalDateTime.class)))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(time(), TypeToken.of(LocalDateTime.class)))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(timestamp(), TypeToken.of(LocalDateTime.class)))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(timestamp(), TypeToken.of(LocalDate.class)))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(timestamp(), TypeToken.of(LocalTime.class)))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
  }

  @Test
  public void should_return_codecs_for_tokenizable_fields() throws Exception {

    DSBulkConfig config = new DefaultDSBulkConfig(ConfigFactory.load().getConfig("dsbulk.codec"));
    CodecSettings settings = new CodecSettings(config);
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
