/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

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
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.UserType;
import com.datastax.loader.engine.internal.codecs.ExtendedCodecRegistry;
import com.datastax.loader.engine.internal.codecs.NumberToNumberCodec;
import com.datastax.loader.engine.internal.codecs.StringToBigDecimalCodec;
import com.datastax.loader.engine.internal.codecs.StringToBigIntegerCodec;
import com.datastax.loader.engine.internal.codecs.StringToBooleanCodec;
import com.datastax.loader.engine.internal.codecs.StringToByteCodec;
import com.datastax.loader.engine.internal.codecs.StringToDoubleCodec;
import com.datastax.loader.engine.internal.codecs.StringToFloatCodec;
import com.datastax.loader.engine.internal.codecs.StringToInstantCodec;
import com.datastax.loader.engine.internal.codecs.StringToIntegerCodec;
import com.datastax.loader.engine.internal.codecs.StringToListCodec;
import com.datastax.loader.engine.internal.codecs.StringToLocalDateCodec;
import com.datastax.loader.engine.internal.codecs.StringToLocalTimeCodec;
import com.datastax.loader.engine.internal.codecs.StringToLongCodec;
import com.datastax.loader.engine.internal.codecs.StringToMapCodec;
import com.datastax.loader.engine.internal.codecs.StringToSetCodec;
import com.datastax.loader.engine.internal.codecs.StringToShortCodec;
import com.datastax.loader.engine.internal.codecs.StringToTupleCodec;
import com.datastax.loader.engine.internal.codecs.StringToUDTCodec;
import com.datastax.loader.engine.internal.codecs.StringToUUIDCodec;
import com.datastax.loader.engine.internal.codecs.TemporalToTemporalCodec;
import com.typesafe.config.Config;
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

    Config config = ConfigFactory.load().getConfig("datastax-loader.codec");
    CodecSettings settings = new CodecSettings(config);
    ExtendedCodecRegistry codecRegistry = settings.init(cluster);

    assertThat(codecRegistry.codecFor(cboolean(), String.class))
        .isNotNull()
        .isInstanceOf(StringToBooleanCodec.class);
    assertThat(codecRegistry.codecFor(tinyint(), String.class))
        .isNotNull()
        .isInstanceOf(StringToByteCodec.class);
    assertThat(codecRegistry.codecFor(smallint(), String.class))
        .isNotNull()
        .isInstanceOf(StringToShortCodec.class);
    assertThat(codecRegistry.codecFor(cint(), String.class))
        .isNotNull()
        .isInstanceOf(StringToIntegerCodec.class);
    assertThat(codecRegistry.codecFor(bigint(), String.class))
        .isNotNull()
        .isInstanceOf(StringToLongCodec.class);
    assertThat(codecRegistry.codecFor(cfloat(), String.class))
        .isNotNull()
        .isInstanceOf(StringToFloatCodec.class);
    assertThat(codecRegistry.codecFor(cdouble(), String.class))
        .isNotNull()
        .isInstanceOf(StringToDoubleCodec.class);
    assertThat(codecRegistry.codecFor(varint(), String.class))
        .isNotNull()
        .isInstanceOf(StringToBigIntegerCodec.class);
    assertThat(codecRegistry.codecFor(decimal(), String.class))
        .isNotNull()
        .isInstanceOf(StringToBigDecimalCodec.class);
    assertThat(codecRegistry.codecFor(date(), String.class))
        .isNotNull()
        .isInstanceOf(StringToLocalDateCodec.class);
    assertThat(codecRegistry.codecFor(time(), String.class))
        .isNotNull()
        .isInstanceOf(StringToLocalTimeCodec.class);
    assertThat(codecRegistry.codecFor(timestamp(), String.class))
        .isNotNull()
        .isInstanceOf(StringToInstantCodec.class);
    assertThat(codecRegistry.codecFor(uuid(), String.class))
        .isNotNull()
        .isInstanceOf(StringToUUIDCodec.class);
    assertThat(codecRegistry.codecFor(timeuuid(), String.class))
        .isNotNull()
        .isInstanceOf(StringToUUIDCodec.class);
  }

  @Test
  public void should_return_number_converting_codecs() throws Exception {

    Config config = ConfigFactory.load().getConfig("datastax-loader.codec");
    CodecSettings settings = new CodecSettings(config);
    ExtendedCodecRegistry codecRegistry = settings.init(cluster);

    assertThat(codecRegistry.codecFor(tinyint(), Short.class))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(smallint(), Integer.class))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(cint(), Long.class))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(bigint(), Float.class))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(cfloat(), Double.class))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(cdouble(), BigDecimal.class))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(varint(), Integer.class))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(decimal(), Float.class))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
  }

  @Test
  public void should_return_temporal_converting_codecs() throws Exception {

    Config config = ConfigFactory.load().getConfig("datastax-loader.codec");
    CodecSettings settings = new CodecSettings(config);
    ExtendedCodecRegistry codecRegistry = settings.init(cluster);

    assertThat(codecRegistry.codecFor(date(), ZonedDateTime.class))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(time(), ZonedDateTime.class))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(timestamp(), ZonedDateTime.class))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(date(), Instant.class))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(time(), Instant.class))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(date(), LocalDateTime.class))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(time(), LocalDateTime.class))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(timestamp(), LocalDateTime.class))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(timestamp(), LocalDate.class))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(timestamp(), LocalTime.class))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
  }

  @Test
  public void should_return_codecs_for_tokenizable_fields() throws Exception {

    Config config = ConfigFactory.load().getConfig("datastax-loader.codec");
    CodecSettings settings = new CodecSettings(config);
    ExtendedCodecRegistry codecRegistry = settings.init(cluster);

    assertThat(codecRegistry.codecFor(list(cint()), String.class))
        .isNotNull()
        .isInstanceOf(StringToListCodec.class);
    assertThat(codecRegistry.codecFor(set(cdouble()), String.class))
        .isNotNull()
        .isInstanceOf(StringToSetCodec.class);
    assertThat(codecRegistry.codecFor(map(time(), varchar()), String.class))
        .isNotNull()
        .isInstanceOf(StringToMapCodec.class);
    TupleType tupleType = newTupleType(cint(), cdouble());
    assertThat(codecRegistry.codecFor(tupleType, String.class))
        .isNotNull()
        .isInstanceOf(StringToTupleCodec.class);
    UserType udtType = newUserType(newField("f1", cint()), newField("f2", cdouble()));
    assertThat(codecRegistry.codecFor(udtType, String.class))
        .isNotNull()
        .isInstanceOf(StringToUDTCodec.class);
  }
}
