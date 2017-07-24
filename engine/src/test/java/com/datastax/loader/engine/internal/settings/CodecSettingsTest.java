/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.settings;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.Configuration;
import com.datastax.driver.core.DataType;
import com.datastax.loader.engine.internal.codecs.NumberToNumberCodec;
import com.datastax.loader.engine.internal.codecs.StringToBigDecimalCodec;
import com.datastax.loader.engine.internal.codecs.StringToBigIntegerCodec;
import com.datastax.loader.engine.internal.codecs.StringToBooleanCodec;
import com.datastax.loader.engine.internal.codecs.StringToByteCodec;
import com.datastax.loader.engine.internal.codecs.StringToDoubleCodec;
import com.datastax.loader.engine.internal.codecs.StringToFloatCodec;
import com.datastax.loader.engine.internal.codecs.StringToInstantCodec;
import com.datastax.loader.engine.internal.codecs.StringToIntegerCodec;
import com.datastax.loader.engine.internal.codecs.StringToLocalDateCodec;
import com.datastax.loader.engine.internal.codecs.StringToLocalTimeCodec;
import com.datastax.loader.engine.internal.codecs.StringToLongCodec;
import com.datastax.loader.engine.internal.codecs.StringToShortCodec;
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

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** */
public class CodecSettingsTest {

  private Cluster cluster;

  private CodecRegistry codecRegistry;

  @Before
  public void setUp() throws Exception {
    cluster = mock(Cluster.class);
    codecRegistry = new CodecRegistry();
    Configuration configuration = mock(Configuration.class);
    when(cluster.getConfiguration()).thenReturn(configuration);
    when(configuration.getCodecRegistry()).thenReturn(codecRegistry);
  }

  @Test
  public void should_register_codecs() throws Exception {

    Config config = ConfigFactory.load().getConfig("datastax-loader.codec");
    CodecSettings settings = new CodecSettings(config);
    settings.registerCodecs(cluster);

    assertThat(codecRegistry.codecFor(DataType.cboolean(), String.class))
        .isNotNull()
        .isInstanceOf(StringToBooleanCodec.class);
    assertThat(codecRegistry.codecFor(DataType.tinyint(), String.class))
        .isNotNull()
        .isInstanceOf(StringToByteCodec.class);
    assertThat(codecRegistry.codecFor(DataType.smallint(), String.class))
        .isNotNull()
        .isInstanceOf(StringToShortCodec.class);
    assertThat(codecRegistry.codecFor(DataType.cint(), String.class))
        .isNotNull()
        .isInstanceOf(StringToIntegerCodec.class);
    assertThat(codecRegistry.codecFor(DataType.bigint(), String.class))
        .isNotNull()
        .isInstanceOf(StringToLongCodec.class);
    assertThat(codecRegistry.codecFor(DataType.cfloat(), String.class))
        .isNotNull()
        .isInstanceOf(StringToFloatCodec.class);
    assertThat(codecRegistry.codecFor(DataType.cdouble(), String.class))
        .isNotNull()
        .isInstanceOf(StringToDoubleCodec.class);
    assertThat(codecRegistry.codecFor(DataType.varint(), String.class))
        .isNotNull()
        .isInstanceOf(StringToBigIntegerCodec.class);
    assertThat(codecRegistry.codecFor(DataType.decimal(), String.class))
        .isNotNull()
        .isInstanceOf(StringToBigDecimalCodec.class);
    assertThat(codecRegistry.codecFor(DataType.date(), String.class))
        .isNotNull()
        .isInstanceOf(StringToLocalDateCodec.class);
    assertThat(codecRegistry.codecFor(DataType.time(), String.class))
        .isNotNull()
        .isInstanceOf(StringToLocalTimeCodec.class);
    assertThat(codecRegistry.codecFor(DataType.timestamp(), String.class))
        .isNotNull()
        .isInstanceOf(StringToInstantCodec.class);
    assertThat(codecRegistry.codecFor(DataType.uuid(), String.class))
        .isNotNull()
        .isInstanceOf(StringToUUIDCodec.class);
    assertThat(codecRegistry.codecFor(DataType.timeuuid(), String.class))
        .isNotNull()
        .isInstanceOf(StringToUUIDCodec.class);

    assertThat(codecRegistry.codecFor(DataType.tinyint(), Short.class))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(DataType.smallint(), Integer.class))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(DataType.cint(), Long.class))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(DataType.bigint(), Float.class))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(DataType.cfloat(), Double.class))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(DataType.cdouble(), BigDecimal.class))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(DataType.varint(), Integer.class))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);
    assertThat(codecRegistry.codecFor(DataType.decimal(), Float.class))
        .isNotNull()
        .isInstanceOf(NumberToNumberCodec.class);

    assertThat(codecRegistry.codecFor(DataType.date(), ZonedDateTime.class))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(DataType.time(), ZonedDateTime.class))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(DataType.timestamp(), ZonedDateTime.class))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(DataType.date(), Instant.class))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(DataType.time(), Instant.class))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(DataType.date(), LocalDateTime.class))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(DataType.time(), LocalDateTime.class))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(DataType.timestamp(), LocalDateTime.class))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(DataType.timestamp(), LocalDate.class))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
    assertThat(codecRegistry.codecFor(DataType.timestamp(), LocalTime.class))
        .isNotNull()
        .isInstanceOf(TemporalToTemporalCodec.class);
  }
}
