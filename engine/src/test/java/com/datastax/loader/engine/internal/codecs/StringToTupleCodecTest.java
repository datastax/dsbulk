/*
 * Copyright (C) 2017 DataStax Inc.
 *
 * This software can be used solely with DataStax Enterprise. Please consult the license at
 * http://www.datastax.com/terms/datastax-dse-driver-license-terms
 */
package com.datastax.loader.engine.internal.codecs;

import static com.datastax.driver.core.DataType.timestamp;
import static com.datastax.driver.core.DataType.varchar;
import static com.datastax.driver.core.DriverCoreTestHooks.newTupleType;
import static com.datastax.driver.core.ProtocolVersion.V4;
import static com.datastax.loader.engine.internal.Assertions.assertThat;
import static com.datastax.loader.engine.internal.settings.CodecSettings.CQL_DATE_TIME_FORMAT;

import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.TupleType;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.extras.codecs.jdk8.InstantCodec;
import com.google.common.collect.Lists;
import java.time.Instant;
import java.util.List;
import org.junit.Before;
import org.junit.Test;

public class StringToTupleCodecTest {

  private StringToTupleCodec codec;
  private TupleType tupleType;

  @Before
  public void setUp() throws Exception {
    CodecRegistry codecRegistry = new CodecRegistry().register(InstantCodec.instance);
    tupleType = newTupleType(V4, codecRegistry, timestamp(), varchar());
    ConvertingCodec eltCodec1 = new StringToInstantCodec(CQL_DATE_TIME_FORMAT);
    ConvertingCodec eltCodec2 = new ExtendedCodecRegistry.StringToStringCodec(TypeCodec.varchar());
    @SuppressWarnings("unchecked")
    List<ConvertingCodec<String, Object>> eltCodecs = Lists.newArrayList(eltCodec1, eltCodec2);
    codec = new StringToTupleCodec(TypeCodec.tuple(tupleType), eltCodecs, ",");
  }

  @Test
  public void should_convert_from_valid_input() throws Exception {
    assertThat(codec)
        .convertsFrom("2016-07-24T20:34:12.999,+01:00")
        .to(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFrom(" 2016-07-24T20:34:12.999 , +01:00 ")
        .to(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFrom("2016-07-24T20:34:12.999Z,+01:00")
        .to(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .convertsFrom(",")
        .to(tupleType.newValue(null, null))
        .convertsFrom(null)
        .to(null)
        .convertsFrom("")
        .to(null);
  }

  @Test
  public void should_convert_to_valid_input() throws Exception {
    assertThat(codec)
        .convertsTo(tupleType.newValue(Instant.parse("2016-07-24T20:34:12.999Z"), "+01:00"))
        .from("2016-07-24T20:34:12.999Z,+01:00")
        .convertsTo(tupleType.newValue(null, null))
        .from(",")
        .convertsTo(null)
        .from(null);
  }

  @Test
  public void should_not_convert_from_invalid_input() throws Exception {
    assertThat(codec).cannotConvertFrom("not a valid input").cannotConvertFrom("not a,valid input");
  }
}
